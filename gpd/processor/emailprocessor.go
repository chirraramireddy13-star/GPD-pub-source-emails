package processor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"time"

	"gpd/aws"
	"gpd/config"
	"gpd/database"
	"gpd/email"
	"gpd/ftp"
)

const (
	receiveRetryAttempts        = 3
	processingRetryAttempts     = 3
	receiveRetryDelay           = 2 * time.Second
	transientStepRetryDelay     = 1 * time.Second
	maxRetryLoggedAttemptSuffix = "attempt %d/%d"
)

type failureKind string

const (
	failureSQSReceive     failureKind = "sqs receive failure"
	failureSQSDelete      failureKind = "sqs delete failure"
	failureMalformedSQS   failureKind = "malformed sqs message"
	failureS3Download     failureKind = "s3 download failure"
	failureCorruptedInput failureKind = "corrupted input file"
	failureRedshift       failureKind = "redshift connection/query failure"
	failureFileGeneration failureKind = "file-generation failure"
	failureFTPUpload      failureKind = "ftp connection/upload failure"
)

type processingFailure struct {
	kind      failureKind
	retryable bool
	err       error
}

func (f *processingFailure) Error() string {
	return fmt.Sprintf("%s: %v", f.kind, f.err)
}

func (f *processingFailure) Unwrap() error {
	return f.err
}

type EmailProcessor struct {
	logger            *log.Logger
	s3Client          *aws.S3Client
	sqsClient         *aws.SQSClient
	redshiftConn      *database.RedshiftConnection
	ftpClient         *ftp.Client
	expectedBucket    string
	batchSettings     email.BatchSettings
	redshiftChunkSize int
	maxMessages       int
	longPollWaitTime  int
}

type messageProcessingState struct {
	message             aws.SQSMessage
	startedAt           time.Time
	objectRef           email.S3ObjectRef
	extractedEmails     []string
	extractionMetrics   email.ExtractionMetrics
	uniqueEmails        []string
	existingEmails      map[string]struct{}
	newEmails           []string
	verificationBatches []email.EmailVerificationRequestBatch
	createdFiles        []string
	uploadedPaths       []string
	duplicateRemoved    int
	detectedFormat      string
}

func NewEmailProcessor(cfg *config.Config, logger *log.Logger, s3Client *aws.S3Client, sqsClient *aws.SQSClient, redshiftConn *database.RedshiftConnection, ftpClient *ftp.Client) (*EmailProcessor, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config is nil")
	}
	if logger == nil {
		return nil, fmt.Errorf("logger is nil")
	}
	if s3Client == nil {
		return nil, fmt.Errorf("s3 client is nil")
	}
	if sqsClient == nil {
		return nil, fmt.Errorf("sqs client is nil")
	}
	if redshiftConn == nil {
		return nil, fmt.Errorf("redshift connection is nil")
	}
	if ftpClient == nil {
		return nil, fmt.Errorf("ftp client is nil")
	}

	batchSettings, err := email.NewBatchSettings(cfg.Batch)
	if err != nil {
		return nil, fmt.Errorf("invalid batch settings: %w", err)
	}

	return &EmailProcessor{
		logger:            logger,
		s3Client:          s3Client,
		sqsClient:         sqsClient,
		redshiftConn:      redshiftConn,
		ftpClient:         ftpClient,
		expectedBucket:    cfg.AWS.S3Bucket,
		batchSettings:     batchSettings,
		redshiftChunkSize: cfg.Redshift.QueryChunkSize,
		maxMessages:       cfg.AWS.SQSMaxMessages,
		longPollWaitTime:  cfg.AWS.SQSWaitSeconds,
	}, nil
}

func (p *EmailProcessor) PollSQS(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		messages, err := p.receiveMessages(ctx)
		if err != nil {
			p.logger.Printf("%s: %v", failureSQSReceive, err)
			continue
		}

		if len(messages) == 0 {
			continue
		}

		for _, msg := range messages {
			if err := p.processMessage(ctx, msg); err != nil {
				p.logMessageFailure(msg.MessageID, err)
				continue
			}

			p.logger.Printf("message id=%s processing succeeded", msg.MessageID)
		}
	}
}

func (p *EmailProcessor) processMessage(ctx context.Context, msg aws.SQSMessage) error {
	state, err := p.prepareMessageProcessingState(ctx, msg)
	if err != nil {
		return err
	}

	p.logBatchSummary(state)

	if len(state.verificationBatches) == 0 {
		return p.completeWithoutRequestFiles(ctx, state)
	}

	if err := p.createAndUploadRequestFiles(ctx, state); err != nil {
		return err
	}

	if err := p.deleteMessageWithRetry(ctx, state.message); err != nil {
		return err
	}

	p.logProcessingMetrics(state)
	return nil
}

func (p *EmailProcessor) prepareMessageProcessingState(ctx context.Context, msg aws.SQSMessage) (*messageProcessingState, error) {
	state := &messageProcessingState{
		message:   msg,
		startedAt: time.Now(),
	}

	objectRef, err := email.ParseS3ObjectFromSQSMessage(msg.Body)
	if err != nil {
		return nil, newProcessingFailure(failureMalformedSQS, false, err)
	}
	if err := email.ValidateExpectedFirehoseObject(objectRef, p.expectedBucket); err != nil {
		return nil, newProcessingFailure(failureMalformedSQS, false, err)
	}
	state.objectRef = objectRef
	state.detectedFormat = email.InferFirehoseFormatFromObjectKey(objectRef.Key)

	p.logger.Printf("accepted message id=%s bucket=%s key=%s formatHint=%s", msg.MessageID, objectRef.Bucket, objectRef.Key, state.detectedFormat)

	objStream, err := p.getObjectWithRetry(ctx, state.objectRef)
	if err != nil {
		return nil, err
	}
	defer objStream.Close()

	state.extractedEmails, state.extractionMetrics, state.detectedFormat, err = email.ExtractEmailsFromFirehoseStreamWithDetectedFormat(objStream, state.detectedFormat)
	if err != nil {
		return nil, newProcessingFailure(failureCorruptedInput, false, err)
	}

	state.uniqueEmails = email.DeduplicateEmails(state.extractedEmails)
	state.duplicateRemoved = len(state.extractedEmails) - len(state.uniqueEmails)

	state.existingEmails, err = p.lookupExistingEmailsWithRetry(ctx, state.uniqueEmails)
	if err != nil {
		return nil, err
	}

	state.newEmails = email.FilterAlreadyExistingEmails(state.uniqueEmails, state.existingEmails)
	state.verificationBatches = email.SplitIntoVerificationBatches(state.newEmails, p.batchSettings)

	return state, nil
}

func (p *EmailProcessor) logBatchSummary(state *messageProcessingState) {
	p.logger.Printf(
		"message id=%s extracted valid emails=%d unique emails=%d existing emails=%d new emails=%d verification batches=%d batch size=%d",
		state.message.MessageID,
		len(state.extractedEmails),
		len(state.uniqueEmails),
		len(state.existingEmails),
		len(state.newEmails),
		len(state.verificationBatches),
		p.batchSettings.Size,
	)

	for _, batch := range state.verificationBatches {
		p.logger.Printf(
			"message id=%s verification batch=%d batch emails=%d",
			state.message.MessageID,
			batch.BatchNumber,
			len(batch.Emails),
		)
	}
}

func (p *EmailProcessor) completeWithoutRequestFiles(ctx context.Context, state *messageProcessingState) error {
	if err := p.deleteMessageWithRetry(ctx, state.message); err != nil {
		return err
	}

	p.logger.Printf("message id=%s has no new emails; no verification batches required", state.message.MessageID)
	p.logProcessingMetrics(state)
	return nil
}

func (p *EmailProcessor) createAndUploadRequestFiles(ctx context.Context, state *messageProcessingState) error {
	createdFiles, err := p.createRequestFilesWithRetry(ctx, state.verificationBatches, state.detectedFormat)
	if err != nil {
		return err
	}
	if len(createdFiles) != len(state.verificationBatches) {
		return newProcessingFailure(failureFileGeneration, false, fmt.Errorf(
			"incomplete request file creation: expected %d files, got %d",
			len(state.verificationBatches),
			len(createdFiles),
		))
	}
	state.createdFiles = createdFiles

	for _, filePath := range state.createdFiles {
		p.logger.Printf("message id=%s created verification request file=%s", state.message.MessageID, filePath)
	}

	uploadedPaths, err := p.uploadFilesWithRetry(ctx, state.createdFiles)
	if err != nil {
		return err
	}
	if len(uploadedPaths) != len(state.createdFiles) {
		return newProcessingFailure(failureFTPUpload, false, fmt.Errorf(
			"incomplete ftp upload: expected %d uploaded files, got %d",
			len(state.createdFiles),
			len(uploadedPaths),
		))
	}
	state.uploadedPaths = uploadedPaths

	for _, remotePath := range state.uploadedPaths {
		p.logger.Printf("message id=%s uploaded verification request remote path=%s", state.message.MessageID, remotePath)
	}

	return nil
}

func (p *EmailProcessor) logProcessingMetrics(state *messageProcessingState) {
	p.logger.Printf(
		"processing metrics: s3 file processed=%s source format=%s total records=%d invalid records=%d duplicate records removed=%d unique emails=%d emails already in redshift=%d new emails=%d number of request batches created=%d emails submitted=%d ftp files uploaded=%d processing duration=%s",
		state.objectRef.Key,
		state.detectedFormat,
		state.extractionMetrics.TotalRecords,
		state.extractionMetrics.InvalidRecords,
		state.duplicateRemoved,
		len(state.uniqueEmails),
		len(state.existingEmails),
		len(state.newEmails),
		len(state.verificationBatches),
		countEmailsInBatches(state.verificationBatches),
		len(state.uploadedPaths),
		time.Since(state.startedAt).String(),
	)
}

func countEmailsInBatches(batches []email.EmailVerificationRequestBatch) int {
	total := 0
	for _, batch := range batches {
		total += len(batch.Emails)
	}
	return total
}

func (p *EmailProcessor) receiveMessages(ctx context.Context) ([]aws.SQSMessage, error) {
	var messages []aws.SQSMessage
	err := p.runRetryStep(ctx, failureSQSReceive, receiveRetryAttempts, receiveRetryDelay, nil, func(attempt int) error {
		result, err := p.sqsClient.ReceiveMessage(ctx, aws.ReceiveMessageInput{
			MaxNumberOfMessages: p.maxMessages,
			WaitTimeSeconds:     p.longPollWaitTime,
		})
		if err != nil {
			return err
		}
		messages = result
		return nil
	})
	if err != nil {
		return nil, err
	}

	return messages, nil
}

func (p *EmailProcessor) getObjectWithRetry(ctx context.Context, objectRef email.S3ObjectRef) (io.ReadCloser, error) {
	var stream io.ReadCloser
	err := p.runRetryStep(ctx, failureS3Download, processingRetryAttempts, transientStepRetryDelay, func(attempt int, err error) {
		p.logger.Printf("message s3 key=%s %s %s: %v", objectRef.Key, failureS3Download, fmt.Sprintf(maxRetryLoggedAttemptSuffix, attempt, processingRetryAttempts), err)
	}, func(attempt int) error {
		result, err := p.s3Client.GetObject(ctx, aws.GetObjectInput{
			Bucket: objectRef.Bucket,
			Key:    objectRef.Key,
		})
		if err != nil {
			return err
		}
		stream = result
		return nil
	})
	if err != nil {
		return nil, newProcessingFailure(failureS3Download, true, err)
	}

	return stream, nil
}

func (p *EmailProcessor) lookupExistingEmailsWithRetry(ctx context.Context, uniqueEmails []string) (map[string]struct{}, error) {
	var existing map[string]struct{}
	err := p.runRetryStep(ctx, failureRedshift, processingRetryAttempts, transientStepRetryDelay, nil, func(attempt int) error {
		result, err := p.redshiftConn.ExistingEmails(ctx, uniqueEmails, p.redshiftChunkSize)
		if err != nil {
			return err
		}
		existing = result
		return nil
	})
	if err != nil {
		return nil, newProcessingFailure(failureRedshift, true, err)
	}

	return existing, nil
}

func (p *EmailProcessor) createRequestFilesWithRetry(ctx context.Context, verificationBatches []email.EmailVerificationRequestBatch, outputFormat string) ([]string, error) {
	var createdFiles []string
	err := p.runRetryStep(ctx, failureFileGeneration, processingRetryAttempts, transientStepRetryDelay, nil, func(attempt int) error {
		result, err := email.CreateVerificationRequestFiles(verificationBatches, p.batchSettings, outputFormat)
		if err != nil {
			return err
		}
		createdFiles = result
		return nil
	})
	if err != nil {
		return nil, newProcessingFailure(failureFileGeneration, true, err)
	}

	return createdFiles, nil
}

func (p *EmailProcessor) uploadFilesWithRetry(ctx context.Context, createdFiles []string) ([]string, error) {
	var uploaded []string
	err := p.runRetryStep(ctx, failureFTPUpload, processingRetryAttempts, transientStepRetryDelay, nil, func(attempt int) error {
		result, err := p.ftpClient.UploadFiles(ctx, createdFiles)
		if err != nil {
			return err
		}
		uploaded = result
		return nil
	})
	if err != nil {
		return nil, newProcessingFailure(failureFTPUpload, true, err)
	}

	return uploaded, nil
}

func (p *EmailProcessor) deleteMessageWithRetry(ctx context.Context, msg aws.SQSMessage) error {
	if msg.ReceiptHandle == "" {
		return newProcessingFailure(failureSQSDelete, false, fmt.Errorf("missing receipt handle for message id=%s", msg.MessageID))
	}

	err := p.runRetryStep(ctx, failureSQSDelete, processingRetryAttempts, transientStepRetryDelay, func(attempt int, err error) {
		p.logger.Printf("message id=%s %s %s: %v", msg.MessageID, failureSQSDelete, fmt.Sprintf(maxRetryLoggedAttemptSuffix, attempt, processingRetryAttempts), err)
	}, func(attempt int) error {
		err := p.sqsClient.DeleteMessage(ctx, aws.DeleteMessageInput{
			ReceiptHandle: msg.ReceiptHandle,
		})
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return newProcessingFailure(failureSQSDelete, true, err)
	}

	p.logger.Printf("message id=%s deleted from sqs", msg.MessageID)
	return nil
}

func (p *EmailProcessor) logMessageFailure(messageID string, err error) {
	var failure *processingFailure
	if errors.As(err, &failure) {
		p.logger.Printf("message id=%s %s retryable=%t: %v", messageID, failure.kind, failure.retryable, failure.err)
		return
	}

	p.logger.Printf("message id=%s processing failed: %v", messageID, err)
}

func (p *EmailProcessor) runRetryStep(
	ctx context.Context,
	kind failureKind,
	attempts int,
	delay time.Duration,
	logAttempt func(attempt int, err error),
	operation func(attempt int) error,
) error {
	return retryOperation(ctx, attempts, delay, func(attempt int) error {
		err := operation(attempt)
		if err != nil {
			if logAttempt != nil {
				logAttempt(attempt, err)
			} else {
				p.logger.Printf("%s %s: %v", kind, fmt.Sprintf(maxRetryLoggedAttemptSuffix, attempt, attempts), err)
			}
		}

		return err
	})
}

func newProcessingFailure(kind failureKind, retryable bool, err error) error {
	return &processingFailure{
		kind:      kind,
		retryable: retryable,
		err:       err,
	}
}

func retryOperation(ctx context.Context, attempts int, delay time.Duration, operation func(attempt int) error) error {
	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		if err := operation(attempt); err != nil {
			lastErr = err
			if attempt == attempts {
				break
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
			continue
		}

		return nil
	}

	return lastErr
}

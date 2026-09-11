package main

import (
	"context"
	"fmt"
	"log"

	"gpd/aws"
	"gpd/config"
	"gpd/database"
	"gpd/ftp"
	"gpd/logger"
	"gpd/processor"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("application initialization failed: %v", err)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}

	appLogger, err := logger.Init(cfg.Log)
	if err != nil {
		return fmt.Errorf("initialize logger: %w", err)
	}

	s3Client, err := aws.NewS3Client(cfg.AWS)
	if err != nil {
		return fmt.Errorf("initialize s3 client: %w", err)
	}

	sqsClient, err := aws.NewSQSClient(cfg.AWS)
	if err != nil {
		return fmt.Errorf("initialize sqs client: %w", err)
	}

	redshiftConn, err := database.NewRedshiftConnection(cfg.Redshift)
	if err != nil {
		return fmt.Errorf("initialize redshift connection: %w", err)
	}

	ftpClientConfig, err := ftp.NewClientConfig(cfg.FTP)
	if err != nil {
		return fmt.Errorf("initialize ftp/sftp config: %w", err)
	}
	ftpClient := ftp.NewClient(*ftpClientConfig)

	batchSize := cfg.Batch.BatchSize
	filePrefix := cfg.Batch.FileNamePrefix
	fileExtension := cfg.Batch.FileNameExtension

	appLogger.Printf(
		"initialized app env=%s awsRegion=%s s3Bucket=%s sqsQueue=%s redshiftHost=%s ftpHost=%s ftpProtocol=%s batchSize=%d filePrefix=%s fileExtension=%s firehoseFormat=%s redshiftQueryChunkSize=%d",
		cfg.Environment,
		s3Client.Region,
		s3Client.Bucket,
		sqsClient.QueueURL,
		redshiftConn.Host,
		ftpClientConfig.Host,
		ftpClientConfig.Protocol,
		batchSize,
		filePrefix,
		fileExtension,
		cfg.Batch.OutputFormat,
		cfg.Redshift.QueryChunkSize,
	)

	emailProcessor, err := processor.NewEmailProcessor(cfg, appLogger, s3Client, sqsClient, redshiftConn, ftpClient)
	if err != nil {
		return fmt.Errorf("initialize email processor: %w", err)
	}

	appLogger.Printf(
		"starting continuous SQS poll queue=%s maxMessages=%d waitTimeSeconds=%d",
		sqsClient.QueueURL,
		cfg.AWS.SQSMaxMessages,
		cfg.AWS.SQSWaitSeconds,
	)

	if err := emailProcessor.PollSQS(context.Background()); err != nil {
		return fmt.Errorf("poll sqs: %w", err)
	}

	return nil
}

package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

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
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

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
	defer redshiftConn.Close()

	ftpClientConfig, err := ftp.NewClientConfig(cfg.FTP)
	if err != nil {
		return fmt.Errorf("initialize ftp/sftp config: %w", err)
	}
	ftpClient := ftp.NewClient(*ftpClientConfig)

	batchSize := cfg.Batch.BatchSize

	appLogger.Printf(
		"initialized app env=%s awsRegion=%s s3Bucket=%s sqsQueue=%s redshiftHost=%s redshiftSchema=%s redshiftEmailTable=%s redshiftEmailColumn=%s ftpHost=%s ftpProtocol=%s batchSize=%d redshiftQueryChunkSize=%d",
		cfg.Environment,
		s3Client.Region,
		s3Client.Bucket,
		sqsClient.QueueURL,
		redshiftConn.Host,
		redshiftConn.Schema,
		redshiftConn.EmailTable,
		redshiftConn.EmailColumn,
		ftpClientConfig.Host,
		ftpClientConfig.Protocol,
		batchSize,
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

	if err := emailProcessor.PollSQS(ctx); err != nil {
		return fmt.Errorf("poll sqs: %w", err)
	}

	appLogger.Printf("shutdown complete: redshift pool closed")

	return nil
}

package aws

import (
	"context"
	"fmt"

	"gpd/config"
)

type SQSClient struct {
	Region   string
	QueueURL string
	receiver SQSReceiver
	deleter  SQSDeleter
}

type ReceiveMessageInput struct {
	MaxNumberOfMessages int
	WaitTimeSeconds     int
}

type SQSMessage struct {
	MessageID     string
	ReceiptHandle string
	Body          string
}

type SQSReceiver func(ctx context.Context, queueURL string, input ReceiveMessageInput) ([]SQSMessage, error)

type DeleteMessageInput struct {
	ReceiptHandle string
}

type SQSDeleter func(ctx context.Context, queueURL string, input DeleteMessageInput) error

func NewSQSClient(cfg config.AWSConfig) (*SQSClient, error) {
	if cfg.Region == "" {
		return nil, fmt.Errorf("AWS_REGION is required")
	}
	if cfg.SQSQueueURL == "" {
		return nil, fmt.Errorf("AWS_SQS_QUEUE_URL is required")
	}

	return &SQSClient{
		Region:   cfg.Region,
		QueueURL: cfg.SQSQueueURL,
		receiver: defaultReceiver,
		deleter:  defaultDeleter,
	}, nil
}

func (c *SQSClient) SetReceiver(receiver SQSReceiver) {
	if receiver == nil {
		c.receiver = defaultReceiver
		return
	}
	c.receiver = receiver
}

func (c *SQSClient) SetDeleter(deleter SQSDeleter) {
	if deleter == nil {
		c.deleter = defaultDeleter
		return
	}
	c.deleter = deleter
}

func (c *SQSClient) ReceiveMessage(ctx context.Context, input ReceiveMessageInput) ([]SQSMessage, error) {
	if c.QueueURL == "" {
		return nil, fmt.Errorf("AWS_SQS_QUEUE_URL is required")
	}
	if input.MaxNumberOfMessages < 1 || input.MaxNumberOfMessages > 10 {
		return nil, fmt.Errorf("invalid MaxNumberOfMessages: %d (allowed 1-10)", input.MaxNumberOfMessages)
	}
	if input.WaitTimeSeconds < 1 || input.WaitTimeSeconds > 20 {
		return nil, fmt.Errorf("invalid WaitTimeSeconds: %d (allowed 1-20)", input.WaitTimeSeconds)
	}
	if c.receiver == nil {
		c.receiver = defaultReceiver
	}

	return c.receiver(ctx, c.QueueURL, input)
}

func (c *SQSClient) DeleteMessage(ctx context.Context, input DeleteMessageInput) error {
	if c.QueueURL == "" {
		return fmt.Errorf("AWS_SQS_QUEUE_URL is required")
	}
	if input.ReceiptHandle == "" {
		return fmt.Errorf("receipt handle is required")
	}
	if c.deleter == nil {
		c.deleter = defaultDeleter
	}

	return c.deleter(ctx, c.QueueURL, input)
}

func defaultReceiver(_ context.Context, _ string, _ ReceiveMessageInput) ([]SQSMessage, error) {
	return []SQSMessage{}, nil
}

func defaultDeleter(_ context.Context, _ string, _ DeleteMessageInput) error {
	return fmt.Errorf("sqs deleter is not configured")
}

package aws

import (
	"context"
	"fmt"
	"io"

	awscfg "github.com/aws/aws-sdk-go-v2/config"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"gpd/config"
)

type S3Client struct {
	Region string
	Bucket string
	getter S3Getter
}

type GetObjectInput struct {
	Bucket string
	Key    string
}

type S3Getter func(ctx context.Context, bucket string, key string) (io.ReadCloser, error)

func NewS3Client(cfg config.AWSConfig) (*S3Client, error) {
	if cfg.Region == "" {
		return nil, fmt.Errorf("AWS_REGION is required")
	}
	if cfg.S3Bucket == "" {
		return nil, fmt.Errorf("AWS_S3_BUCKET is required")
	}

	awsConfig, err := awscfg.LoadDefaultConfig(context.Background(), awscfg.WithRegion(cfg.Region))
	if err != nil {
		return nil, fmt.Errorf("load aws config for s3: %w", err)
	}

	s3API := awss3.NewFromConfig(awsConfig)

	return &S3Client{
		Region: cfg.Region,
		Bucket: cfg.S3Bucket,
		getter: newS3Getter(s3API),
	}, nil
}

func (c *S3Client) SetGetter(getter S3Getter) {
	if getter == nil {
		c.getter = defaultGetter
		return
	}
	c.getter = getter
}

func (c *S3Client) GetObject(ctx context.Context, input GetObjectInput) (io.ReadCloser, error) {
	bucket := input.Bucket
	if bucket == "" {
		bucket = c.Bucket
	}
	if bucket == "" {
		return nil, fmt.Errorf("AWS_S3_BUCKET is required")
	}
	if input.Key == "" {
		return nil, fmt.Errorf("s3 object key is required")
	}
	if c.getter == nil {
		c.getter = defaultGetter
	}

	return c.getter(ctx, bucket, input.Key)
}

func defaultGetter(_ context.Context, _, _ string) (io.ReadCloser, error) {
	return nil, fmt.Errorf("s3 getter is not configured")
}

func newS3Getter(api *awss3.Client) S3Getter {
	return func(ctx context.Context, bucket string, key string) (io.ReadCloser, error) {
		output, err := api.GetObject(ctx, &awss3.GetObjectInput{
			Bucket: &bucket,
			Key:    &key,
		})
		if err != nil {
			return nil, err
		}

		return output.Body, nil
	}
}

package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	Environment string
	Log         LoggingConfig
	AWS         AWSConfig
	Redshift    RedshiftConfig
	FTP         FTPConfig
	Batch       BatchConfig
}

type LoggingConfig struct {
	Level  string
	Format string
}

type AWSConfig struct {
	Region         string
	S3Bucket       string
	SQSQueueURL    string
	SQSMaxMessages int
	SQSWaitSeconds int
}

type RedshiftConfig struct {
	Host           string
	Port           int
	Database       string
	User           string
	Password       string
	SSLMode        string
	QueryChunkSize int
}

type FTPConfig struct {
	Protocol       string
	Host           string
	Port           int
	User           string
	Password       string
	RemotePath     string
	TimeoutSeconds int
}

type BatchConfig struct {
	BatchSize         int
	FileNamePrefix    string
	FileNameExtension string
	OutputFormat      string
	RequestFilePrefix string
	RequestOutputDir  string
}

func Load() (*Config, error) {
	redshiftPort, err := parseIntEnv("REDSHIFT_PORT", 5439)
	if err != nil {
		return nil, err
	}

	ftpPort, err := parseIntEnv("FTP_PORT", 22)
	if err != nil {
		return nil, err
	}

	ftpTimeout, err := parseIntEnv("FTP_TIMEOUT_SECONDS", 30)
	if err != nil {
		return nil, err
	}

	batchSize, err := parseIntEnv("BATCH_SIZE", 100)
	if err != nil {
		return nil, err
	}

	sqsMaxMessages, err := parseIntEnv("SQS_MAX_MESSAGES", 10)
	if err != nil {
		return nil, err
	}

	sqsWaitSeconds, err := parseIntEnv("SQS_WAIT_TIME_SECONDS", 20)
	if err != nil {
		return nil, err
	}

	redshiftQueryChunkSize, err := parseIntEnv("REDSHIFT_QUERY_CHUNK_SIZE", 500)
	if err != nil {
		return nil, err
	}

	cfg := &Config{
		Environment: getEnv("APP_ENV", "dev"),
		Log: LoggingConfig{
			Level:  strings.ToUpper(getEnv("LOG_LEVEL", "INFO")),
			Format: strings.ToLower(getEnv("LOG_FORMAT", "text")),
		},
		AWS: AWSConfig{
			Region:         getEnv("AWS_REGION", ""),
			S3Bucket:       getEnv("AWS_S3_BUCKET", ""),
			SQSQueueURL:    getEnv("AWS_SQS_QUEUE_URL", ""),
			SQSMaxMessages: sqsMaxMessages,
			SQSWaitSeconds: sqsWaitSeconds,
		},
		Redshift: RedshiftConfig{
			Host:           getEnv("REDSHIFT_HOST", ""),
			Port:           redshiftPort,
			Database:       getEnv("REDSHIFT_DATABASE", ""),
			User:           getEnv("REDSHIFT_USER", ""),
			Password:       getEnv("REDSHIFT_PASSWORD", ""),
			SSLMode:        strings.ToLower(getEnv("REDSHIFT_SSLMODE", "require")),
			QueryChunkSize: redshiftQueryChunkSize,
		},
		FTP: FTPConfig{
			Protocol:       strings.ToLower(getEnv("FTP_PROTOCOL", "sftp")),
			Host:           getEnv("FTP_HOST", ""),
			Port:           ftpPort,
			User:           getEnv("FTP_USER", ""),
			Password:       getEnv("FTP_PASSWORD", ""),
			RemotePath:     getEnv("FTP_REMOTE_PATH", "/"),
			TimeoutSeconds: ftpTimeout,
		},
		Batch: BatchConfig{
			BatchSize:         batchSize,
			FileNamePrefix:    getEnv("FILE_NAME_PREFIX", "emails"),
			FileNameExtension: getEnv("FILE_NAME_EXTENSION", ".json"),
			OutputFormat:      strings.ToLower(getEnv("FIREHOSE_OUTPUT_FORMAT", "json")),
			RequestFilePrefix: getEnv("REQUEST_FILE_PREFIX", "batch"),
			RequestOutputDir:  getEnv("REQUEST_OUTPUT_DIR", "out/verification_requests"),
		},
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

func (c *Config) Validate() error {
	missing := make([]string, 0)

	if c.AWS.Region == "" {
		missing = append(missing, "AWS_REGION")
	}
	if c.AWS.S3Bucket == "" {
		missing = append(missing, "AWS_S3_BUCKET")
	}
	if c.AWS.SQSQueueURL == "" {
		missing = append(missing, "AWS_SQS_QUEUE_URL")
	}

	if c.Redshift.Host == "" {
		missing = append(missing, "REDSHIFT_HOST")
	}
	if c.Redshift.Database == "" {
		missing = append(missing, "REDSHIFT_DATABASE")
	}
	if c.Redshift.User == "" {
		missing = append(missing, "REDSHIFT_USER")
	}
	if c.Redshift.Password == "" {
		missing = append(missing, "REDSHIFT_PASSWORD")
	}

	if c.FTP.Host == "" {
		missing = append(missing, "FTP_HOST")
	}
	if c.FTP.User == "" {
		missing = append(missing, "FTP_USER")
	}
	if c.FTP.Password == "" {
		missing = append(missing, "FTP_PASSWORD")
	}

	if c.Batch.FileNamePrefix == "" {
		missing = append(missing, "FILE_NAME_PREFIX")
	}
	if strings.TrimSpace(c.Batch.RequestFilePrefix) == "" {
		missing = append(missing, "REQUEST_FILE_PREFIX")
	}
	if strings.TrimSpace(c.Batch.RequestOutputDir) == "" {
		missing = append(missing, "REQUEST_OUTPUT_DIR")
	}

	if len(missing) > 0 {
		return fmt.Errorf("missing required configuration values: %s", strings.Join(missing, ", "))
	}

	if c.Redshift.Port <= 0 {
		return fmt.Errorf("invalid REDSHIFT_PORT: %d", c.Redshift.Port)
	}
	if c.Redshift.QueryChunkSize <= 0 {
		return fmt.Errorf("invalid REDSHIFT_QUERY_CHUNK_SIZE: %d", c.Redshift.QueryChunkSize)
	}
	if c.FTP.Port <= 0 {
		return fmt.Errorf("invalid FTP_PORT: %d", c.FTP.Port)
	}
	if c.FTP.TimeoutSeconds <= 0 {
		return fmt.Errorf("invalid FTP_TIMEOUT_SECONDS: %d", c.FTP.TimeoutSeconds)
	}
	if c.Batch.BatchSize <= 0 {
		return fmt.Errorf("invalid BATCH_SIZE: %d", c.Batch.BatchSize)
	}
	if c.Batch.OutputFormat != "csv" && c.Batch.OutputFormat != "json" {
		return fmt.Errorf("invalid FIREHOSE_OUTPUT_FORMAT: %s", c.Batch.OutputFormat)
	}
	if c.AWS.SQSMaxMessages < 1 || c.AWS.SQSMaxMessages > 10 {
		return fmt.Errorf("invalid SQS_MAX_MESSAGES: %d (allowed 1-10)", c.AWS.SQSMaxMessages)
	}
	if c.AWS.SQSWaitSeconds < 1 || c.AWS.SQSWaitSeconds > 20 {
		return fmt.Errorf("invalid SQS_WAIT_TIME_SECONDS: %d (allowed 1-20)", c.AWS.SQSWaitSeconds)
	}
	if c.FTP.Protocol != "ftp" && c.FTP.Protocol != "sftp" {
		return fmt.Errorf("invalid FTP_PROTOCOL: %s", c.FTP.Protocol)
	}

	return nil
}

func getEnv(key, defaultValue string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return defaultValue
	}
	return value
}

func parseIntEnv(key string, defaultValue int) (int, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return defaultValue, nil
	}

	parsed, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid integer for %s: %q", key, raw)
	}

	return parsed, nil
}

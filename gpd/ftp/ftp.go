package ftp

import (
	"context"
	"fmt"
	"path"
	"path/filepath"
	"strings"

	"gpd/config"
)

type ClientConfig struct {
	Protocol       string
	Host           string
	Port           int
	User           string
	Password       string
	RemotePath     string
	TimeoutSeconds int
}

type Uploader func(ctx context.Context, cfg ClientConfig, localPath string, remotePath string) error

type Client struct {
	config   ClientConfig
	uploader Uploader
}

func NewClientConfig(cfg config.FTPConfig) (*ClientConfig, error) {
	protocol := strings.ToLower(strings.TrimSpace(cfg.Protocol))
	if protocol != "ftp" && protocol != "sftp" {
		return nil, fmt.Errorf("invalid FTP_PROTOCOL: %s", cfg.Protocol)
	}
	if cfg.Host == "" {
		return nil, fmt.Errorf("FTP_HOST is required")
	}
	if cfg.Port <= 0 {
		return nil, fmt.Errorf("invalid FTP_PORT: %d", cfg.Port)
	}
	if cfg.User == "" {
		return nil, fmt.Errorf("FTP_USER is required")
	}
	if cfg.Password == "" {
		return nil, fmt.Errorf("FTP_PASSWORD is required")
	}
	if cfg.TimeoutSeconds <= 0 {
		return nil, fmt.Errorf("invalid FTP_TIMEOUT_SECONDS: %d", cfg.TimeoutSeconds)
	}

	remotePath := cfg.RemotePath
	if strings.TrimSpace(remotePath) == "" {
		remotePath = "/"
	}

	return &ClientConfig{
		Protocol:       protocol,
		Host:           cfg.Host,
		Port:           cfg.Port,
		User:           cfg.User,
		Password:       cfg.Password,
		RemotePath:     remotePath,
		TimeoutSeconds: cfg.TimeoutSeconds,
	}, nil
}

func NewClient(cfg ClientConfig) *Client {
	return &Client{
		config:   cfg,
		uploader: defaultUploader,
	}
}

func (c *Client) SetUploader(uploader Uploader) {
	if uploader == nil {
		c.uploader = defaultUploader
		return
	}
	c.uploader = uploader
}

func (c *Client) UploadFile(ctx context.Context, localPath string) (string, error) {
	trimmedLocalPath := strings.TrimSpace(localPath)
	if trimmedLocalPath == "" {
		return "", fmt.Errorf("local path is required")
	}

	fileName := filepath.Base(trimmedLocalPath)
	if fileName == "." || fileName == string(filepath.Separator) || fileName == "" {
		return "", fmt.Errorf("invalid local file path: %s", localPath)
	}

	remotePath := path.Join(c.config.RemotePath, fileName)
	if c.uploader == nil {
		c.uploader = defaultUploader
	}

	if err := c.uploader(ctx, c.config, trimmedLocalPath, remotePath); err != nil {
		return "", fmt.Errorf("upload file %s to %s: %w", trimmedLocalPath, remotePath, err)
	}

	return remotePath, nil
}

func (c *Client) UploadFiles(ctx context.Context, localPaths []string) ([]string, error) {
	if len(localPaths) == 0 {
		return []string{}, nil
	}

	uploaded := make([]string, 0, len(localPaths))
	for _, localPath := range localPaths {
		remotePath, err := c.UploadFile(ctx, localPath)
		if err != nil {
			return nil, err
		}
		uploaded = append(uploaded, remotePath)
	}

	return uploaded, nil
}

func defaultUploader(_ context.Context, cfg ClientConfig, localPath string, remotePath string) error {
	return fmt.Errorf("ftp uploader is not configured for %s://%s:%d (%s -> %s)", cfg.Protocol, cfg.Host, cfg.Port, localPath, remotePath)
}

package ftp

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"

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
	uploader := defaultUploader
	if cfg.Protocol == "sftp" {
		uploader = sftpUploader
	}

	return &Client{
		config:   cfg,
		uploader: uploader,
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

func sftpUploader(ctx context.Context, cfg ClientConfig, localPath string, remotePath string) error {
	sshConfig := &ssh.ClientConfig{
		User:            cfg.User,
		Auth:            []ssh.AuthMethod{ssh.Password(cfg.Password)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         0,
	}

	address := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
	sshClient, err := ssh.Dial("tcp", address, sshConfig)
	if err != nil {
		return err
	}
	defer sshClient.Close()

	client, err := sftp.NewClient(sshClient)
	if err != nil {
		return err
	}
	defer client.Close()

	if err := ensureRemoteDir(ctx, client, path.Dir(remotePath)); err != nil {
		return err
	}

	localFile, err := os.Open(localPath)
	if err != nil {
		return err
	}
	defer localFile.Close()

	remoteFile, err := client.Create(remotePath)
	if err != nil {
		return err
	}
	defer remoteFile.Close()

	if _, err := io.Copy(remoteFile, localFile); err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

func ensureRemoteDir(ctx context.Context, client *sftp.Client, remoteDir string) error {
	if remoteDir == "." || remoteDir == "/" || remoteDir == "" {
		return nil
	}

	parts := strings.Split(strings.TrimPrefix(remoteDir, "/"), "/")
	current := ""
	if strings.HasPrefix(remoteDir, "/") {
		current = "/"
	}

	for _, part := range parts {
		if part == "" {
			continue
		}
		current = path.Join(current, part)
		if err := client.Mkdir(current); err != nil && !isSFTPPathExists(client, current) {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}

	return nil
}

func isSFTPPathExists(client *sftp.Client, remotePath string) bool {
	_, err := client.Stat(remotePath)
	return err == nil
}

package email

import (
	"crypto/rand"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"gpd/config"
)

var requestFileSequence uint64

type BatchSettings struct {
	Size              int
	RequestFilePrefix string
	RequestOutputDir  string
}

type EmailVerificationRequestBatch struct {
	BatchNumber int
	Emails      []string
}

func NewBatchSettings(cfg config.BatchConfig) (BatchSettings, error) {
	settings := BatchSettings{
		Size:              cfg.BatchSize,
		RequestFilePrefix: strings.TrimSpace(cfg.RequestFilePrefix),
		RequestOutputDir:  strings.TrimSpace(cfg.RequestOutputDir),
	}

	if settings.Size <= 0 {
		return BatchSettings{}, fmt.Errorf("batch size must be greater than zero")
	}
	if settings.RequestFilePrefix == "" {
		return BatchSettings{}, fmt.Errorf("request file prefix is required")
	}
	if settings.RequestOutputDir == "" {
		return BatchSettings{}, fmt.Errorf("request output dir is required")
	}

	return settings, nil
}

func SplitIntoVerificationBatches(emails []string, settings BatchSettings) []EmailVerificationRequestBatch {
	if len(emails) == 0 || settings.Size <= 0 {
		return []EmailVerificationRequestBatch{}
	}

	batches := make([]EmailVerificationRequestBatch, 0, (len(emails)+settings.Size-1)/settings.Size)
	batchNumber := 1
	for start := 0; start < len(emails); start += settings.Size {
		end := start + settings.Size
		if end > len(emails) {
			end = len(emails)
		}

		chunk := make([]string, end-start)
		copy(chunk, emails[start:end])

		batches = append(batches, EmailVerificationRequestBatch{
			BatchNumber: batchNumber,
			Emails:      chunk,
		})
		batchNumber++
	}

	return batches
}

func CreateVerificationRequestFiles(batches []EmailVerificationRequestBatch, settings BatchSettings, outputFormat string) ([]string, error) {
	if len(batches) == 0 {
		return []string{}, nil
	}

	format := strings.ToLower(strings.TrimSpace(outputFormat))
	if format != "csv" && format != "json" {
		return nil, fmt.Errorf("unsupported request output format: %s", outputFormat)
	}

	if err := os.MkdirAll(settings.RequestOutputDir, 0o755); err != nil {
		return nil, fmt.Errorf("create request output dir: %w", err)
	}

	createdFiles := make([]string, 0, len(batches))
	for _, batch := range batches {
		if len(batch.Emails) == 0 {
			continue
		}

		fileName, err := generateRequestFileName(settings.RequestFilePrefix, format)
		if err != nil {
			return nil, err
		}
		finalPath := filepath.Join(settings.RequestOutputDir, fileName)

		tmpFile, err := os.CreateTemp(settings.RequestOutputDir, "req-*.tmp")
		if err != nil {
			return nil, fmt.Errorf("create temp request file: %w", err)
		}

		writeErr := writeBatchPayload(tmpFile, batch.Emails, format)
		closeErr := tmpFile.Close()
		if writeErr != nil {
			_ = os.Remove(tmpFile.Name())
			return nil, writeErr
		}
		if closeErr != nil {
			_ = os.Remove(tmpFile.Name())
			return nil, fmt.Errorf("close request file: %w", closeErr)
		}

		if err := os.Rename(tmpFile.Name(), finalPath); err != nil {
			_ = os.Remove(tmpFile.Name())
			return nil, fmt.Errorf("move request file into place: %w", err)
		}

		createdFiles = append(createdFiles, filepath.ToSlash(finalPath))
	}

	return createdFiles, nil
}

func generateRequestFileName(prefix string, format string) (string, error) {
	ts := time.Now().UTC().Format("20060102150405")
	sequence := atomic.AddUint64(&requestFileSequence, 1)
	randToken, err := randomHex(4)
	if err != nil {
		return "", fmt.Errorf("generate random token for request filename: %w", err)
	}

	ext := "." + format
	return fmt.Sprintf("%s_%s%06d_%s%s", prefix, ts, sequence, randToken, ext), nil
}

func randomHex(byteLen int) (string, error) {
	b := make([]byte, byteLen)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func writeBatchPayload(file *os.File, emails []string, format string) error {
	if format == "csv" {
		writer := csv.NewWriter(file)
		if err := writer.Write([]string{"email"}); err != nil {
			return fmt.Errorf("write csv header: %w", err)
		}
		for _, addr := range emails {
			if err := writer.Write([]string{addr}); err != nil {
				return fmt.Errorf("write csv row: %w", err)
			}
		}
		writer.Flush()
		if err := writer.Error(); err != nil {
			return fmt.Errorf("flush csv writer: %w", err)
		}
		return nil
	}

	payload := map[string][]string{"emails": emails}
	encoder := json.NewEncoder(file)
	if err := encoder.Encode(payload); err != nil {
		return fmt.Errorf("write json payload: %w", err)
	}

	return nil
}

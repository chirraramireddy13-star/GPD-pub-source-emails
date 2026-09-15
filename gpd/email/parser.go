package email

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"path"
	"regexp"
	"strings"
)

var emailPattern = regexp.MustCompile(`^[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}$`)

type ExtractionMetrics struct {
	TotalRecords   int
	InvalidRecords int
}

type S3ObjectRef struct {
	Bucket string
	Key    string
}

type sqsEnvelope struct {
	Message string `json:"Message"`
}

type s3Event struct {
	Records []struct {
		EventSource string `json:"eventSource"`
		S3          struct {
			Bucket struct {
				Name string `json:"name"`
			} `json:"bucket"`
			Object struct {
				Key string `json:"key"`
			} `json:"object"`
		} `json:"s3"`
	} `json:"Records"`
}

func ParseS3ObjectFromSQSMessage(body string) (S3ObjectRef, error) {
	eventPayload := strings.TrimSpace(body)
	if eventPayload == "" {
		return S3ObjectRef{}, fmt.Errorf("empty SQS message body")
	}

	var envelope sqsEnvelope
	if err := json.Unmarshal([]byte(eventPayload), &envelope); err == nil && strings.TrimSpace(envelope.Message) != "" {
		eventPayload = envelope.Message
	}

	var event s3Event
	if err := json.Unmarshal([]byte(eventPayload), &event); err != nil {
		return S3ObjectRef{}, fmt.Errorf("decode s3 event: %w", err)
	}
	if len(event.Records) == 0 {
		return S3ObjectRef{}, fmt.Errorf("no records in S3 event")
	}

	record := event.Records[0]
	bucket := strings.TrimSpace(record.S3.Bucket.Name)
	if bucket == "" {
		return S3ObjectRef{}, fmt.Errorf("missing bucket name in S3 event")
	}

	rawKey := strings.TrimSpace(record.S3.Object.Key)
	if rawKey == "" {
		return S3ObjectRef{}, fmt.Errorf("missing object key in S3 event")
	}

	decodedKey, err := decodeS3ObjectKey(rawKey)
	if err != nil {
		return S3ObjectRef{}, err
	}

	return S3ObjectRef{
		Bucket: bucket,
		Key:    decodedKey,
	}, nil
}

func ValidateExpectedFirehoseObject(object S3ObjectRef, expectedBucket string) error {
	if strings.TrimSpace(expectedBucket) == "" {
		return fmt.Errorf("expected bucket is empty")
	}
	if object.Bucket != expectedBucket {
		return fmt.Errorf("unexpected bucket: %s", object.Bucket)
	}
	if strings.TrimSpace(object.Key) == "" {
		return fmt.Errorf("missing object key in S3 event")
	}

	return nil
}

func InferFirehoseFormatFromObjectKey(objectKey string) string {
	ext := strings.ToLower(strings.TrimSpace(path.Ext(objectKey)))
	switch ext {
	case ".csv":
		return "csv"
	case ".json", ".ndjson":
		return "json"
	case ".txt", ".text", ".log":
		return "text"
	default:
		return ""
	}
}

func decodeS3ObjectKey(raw string) (string, error) {
	replaced := strings.ReplaceAll(raw, "+", "%20")
	decoded, err := url.QueryUnescape(replaced)
	if err != nil {
		return "", fmt.Errorf("decode object key: %w", err)
	}

	return decoded, nil
}

func ExtractEmailsFromFirehoseStream(r io.Reader, outputFormat string) ([]string, error) {
	emails, _, err := ExtractEmailsFromFirehoseStreamWithMetrics(r, outputFormat)
	if err != nil {
		return nil, err
	}

	return emails, nil
}

func ExtractEmailsFromFirehoseStreamWithMetrics(r io.Reader, outputFormat string) ([]string, ExtractionMetrics, error) {
	emails, metrics, _, err := ExtractEmailsFromFirehoseStreamWithDetectedFormat(r, outputFormat)
	if err != nil {
		return nil, ExtractionMetrics{}, err
	}

	return emails, metrics, nil
}

func ExtractEmailsFromFirehoseStreamWithDetectedFormat(r io.Reader, outputFormat string) ([]string, ExtractionMetrics, string, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, ExtractionMetrics{}, "", fmt.Errorf("read firehose payload: %w", err)
	}

	trimmedData := strings.TrimSpace(string(data))
	if trimmedData == "" {
		return []string{}, ExtractionMetrics{}, normalizeFormat(outputFormat), nil
	}

	format := strings.ToLower(strings.TrimSpace(outputFormat))
	candidates := buildFormatCandidates(format, trimmedData)
	var parseErrs []string

	for _, candidate := range candidates {
		emails, metrics, err := extractByFormat(candidate, strings.NewReader(trimmedData))
		if err == nil {
			return emails, metrics, candidate, nil
		}
		parseErrs = append(parseErrs, fmt.Sprintf("%s: %v", candidate, err))
	}

	return nil, ExtractionMetrics{}, "", fmt.Errorf("unable to parse firehose payload; attempts=%s", strings.Join(parseErrs, "; "))
}

func normalizeFormat(format string) string {
	trimmed := strings.ToLower(strings.TrimSpace(format))
	if trimmed == "csv" || trimmed == "json" || trimmed == "text" {
		return trimmed
	}
	return ""
}

func buildFormatCandidates(formatHint string, payload string) []string {
	normalized := normalizeFormat(formatHint)
	if normalized == "csv" {
		return []string{"csv", "text", "json"}
	}
	if normalized == "json" {
		return []string{"json", "csv", "text"}
	}
	if normalized == "text" {
		return []string{"text", "csv", "json"}
	}

	if looksLikeJSON(payload) {
		return []string{"json", "csv", "text"}
	}

	return []string{"text", "csv", "json"}
}

func looksLikeJSON(payload string) bool {
	trimmed := strings.TrimSpace(payload)
	if trimmed == "" {
		return false
	}

	return strings.HasPrefix(trimmed, "{") || strings.HasPrefix(trimmed, "[")
}

func extractByFormat(format string, r io.Reader) ([]string, ExtractionMetrics, error) {
	if format == "csv" {
		return extractEmailsFromCSV(r)
	}
	if format == "json" {
		return extractEmailsFromJSON(r)
	}
	if format == "text" {
		return extractEmailsFromPlainText(r)
	}

	return nil, ExtractionMetrics{}, fmt.Errorf("unsupported firehose output format: %s", format)
}

func NormalizeEmail(raw string) string {
	email := strings.TrimSpace(raw)
	email = strings.ToLower(email)
	return email
}

func IsValidEmail(email string) bool {
	if email == "" {
		return false
	}

	return emailPattern.MatchString(email)
}

func extractEmailsFromCSV(r io.Reader) ([]string, ExtractionMetrics, error) {
	reader := csv.NewReader(r)
	headers, err := reader.Read()
	if err != nil {
		if err == io.EOF {
			return []string{}, ExtractionMetrics{}, nil
		}
		return nil, ExtractionMetrics{}, fmt.Errorf("read csv header: %w", err)
	}

	emailIndex := -1
	for i, header := range headers {
		if strings.EqualFold(strings.TrimSpace(header), "email") {
			emailIndex = i
			break
		}
	}
	if emailIndex == -1 {
		return nil, ExtractionMetrics{}, fmt.Errorf("email column not found in csv")
	}

	validEmails := make([]string, 0)
	metrics := ExtractionMetrics{}
	for {
		record, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, ExtractionMetrics{}, fmt.Errorf("read csv record: %w", err)
		}

		metrics.TotalRecords++
		if emailIndex >= len(record) {
			markInvalidRecord(&metrics)
			continue
		}

		appendNormalizedValidEmail(record[emailIndex], &validEmails, &metrics)
	}

	return validEmails, metrics, nil
}

func extractEmailsFromJSON(r io.Reader) ([]string, ExtractionMetrics, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, ExtractionMetrics{}, fmt.Errorf("read json payload: %w", err)
	}

	trimmed := strings.TrimSpace(string(data))
	if trimmed == "" {
		return []string{}, ExtractionMetrics{}, nil
	}

	if strings.HasPrefix(trimmed, "[") {
		var records []map[string]any
		if err := json.Unmarshal([]byte(trimmed), &records); err != nil {
			return nil, ExtractionMetrics{}, fmt.Errorf("decode json array: %w", err)
		}
		return collectEmailsFromMapRecords(records)
	}

	return extractEmailsFromNDJSON(strings.NewReader(trimmed))
}

func extractEmailsFromNDJSON(r io.Reader) ([]string, ExtractionMetrics, error) {
	validEmails := make([]string, 0)
	metrics := ExtractionMetrics{}
	parsedJSONRecords := 0
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		metrics.TotalRecords++

		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			markInvalidRecord(&metrics)
			continue
		}
		parsedJSONRecords++

		rawEmail, ok := record["email"].(string)
		if !ok {
			markInvalidRecord(&metrics)
			continue
		}

		appendNormalizedValidEmail(rawEmail, &validEmails, &metrics)
	}
	if err := scanner.Err(); err != nil {
		return nil, ExtractionMetrics{}, fmt.Errorf("scan ndjson: %w", err)
	}
	if metrics.TotalRecords > 0 && parsedJSONRecords == 0 {
		return nil, ExtractionMetrics{}, fmt.Errorf("payload is not valid ndjson")
	}

	return validEmails, metrics, nil
}

func extractEmailsFromPlainText(r io.Reader) ([]string, ExtractionMetrics, error) {
	validEmails := make([]string, 0)
	metrics := ExtractionMetrics{}
	seenLineWithEmail := false

	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		metrics.TotalRecords++
		lineHadValidEmail := false

		for _, token := range tokenizePlainTextLine(line) {
			normalized := NormalizeEmail(token)
			if IsValidEmail(normalized) {
				validEmails = append(validEmails, normalized)
				lineHadValidEmail = true
			}
		}

		if lineHadValidEmail {
			seenLineWithEmail = true
		} else {
			markInvalidRecord(&metrics)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, ExtractionMetrics{}, fmt.Errorf("scan plain text payload: %w", err)
	}

	if metrics.TotalRecords > 0 && !seenLineWithEmail {
		return nil, ExtractionMetrics{}, fmt.Errorf("payload has no valid plain-text email records")
	}

	return validEmails, metrics, nil
}

func tokenizePlainTextLine(line string) []string {
	separators := func(r rune) bool {
		switch r {
		case ',', ';', '\t', ' ', '|':
			return true
		default:
			return false
		}
	}

	return strings.FieldsFunc(line, separators)
}

func collectEmailsFromMapRecords(records []map[string]any) ([]string, ExtractionMetrics, error) {
	validEmails := make([]string, 0)
	metrics := ExtractionMetrics{}
	for _, record := range records {
		metrics.TotalRecords++

		rawEmail, ok := record["email"].(string)
		if !ok {
			markInvalidRecord(&metrics)
			continue
		}

		appendNormalizedValidEmail(rawEmail, &validEmails, &metrics)
	}

	return validEmails, metrics, nil
}

func appendNormalizedValidEmail(raw string, emails *[]string, metrics *ExtractionMetrics) {
	normalized := NormalizeEmail(raw)
	if normalized == "" || !IsValidEmail(normalized) {
		markInvalidRecord(metrics)
		return
	}

	*emails = append(*emails, normalized)
}

func markInvalidRecord(metrics *ExtractionMetrics) {
	metrics.InvalidRecords++
}

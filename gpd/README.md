# GPD

GPD is a Go-based email processing pipeline that consumes SQS notifications for Firehose-delivered S3 files, extracts and filters email addresses, generates Email Verification Request batch files, and uploads those files to an FTP or SFTP destination.

See [ARCHITECTURE.md](ARCHITECTURE.md) for the end-to-end runtime flow, retry behavior, success criteria, and module responsibilities.

## Current Status

The orchestration flow is implemented in code, including:

- configuration loading and validation
- continuous SQS polling
- SQS event parsing
- S3 object download
- CSV, JSON, and plain-text parsing
- email normalization and validation
- in-memory deduplication
- batched Redshift existence lookup
- filtering already-known emails
- request batch splitting
- request file generation
- FTP or SFTP upload flow
- SQS delete after successful processing
- categorized failure handling and retries
- processing metrics logging

Core integrations are wired in production code:

- AWS SDK v2 for SQS and S3
- pgx/pgxpool for Redshift lookups
- pkg/sftp with x/crypto/ssh for SFTP uploads

Known limitation:

- `FTP_PROTOCOL=ftp` is accepted by configuration but does not have a concrete uploader implementation yet.

## Project Flow

1. Load and validate environment configuration.
2. Initialize logger and service clients.
3. Poll SQS continuously with long polling.
4. Parse the SQS payload and extract S3 bucket and object key.
5. Validate expected bucket and infer source format from object key and payload content.
6. Download the Firehose batch file from S3.
7. Parse records and extract normalized valid emails.
8. Deduplicate emails in memory.
9. Query Redshift in chunks to find already-known emails.
10. Filter existing emails out.
11. Split new emails into verification request batches.
12. Create request batch files on disk.
13. Upload each batch file to FTP or SFTP.
14. Delete the SQS message after the full success path completes.
15. Log processing metrics.
16. Treat the message as successful only after all required uploads and SQS deletion succeed.

## Required Environment Variables

### Application and logging

- `APP_ENV`
- `LOG_LEVEL`
- `LOG_FORMAT`

### AWS

- `AWS_REGION`
- `AWS_S3_BUCKET`
- `AWS_SQS_QUEUE_URL`
- `SQS_MAX_MESSAGES`
- `SQS_WAIT_TIME_SECONDS`

### Redshift

- `REDSHIFT_HOST`
- `REDSHIFT_PORT`
- `REDSHIFT_DATABASE`
- `REDSHIFT_SCHEMA` (optional, default `public`)
- `REDSHIFT_EMAIL_TABLE` (optional, default `emailunique`)
- `REDSHIFT_EMAIL_COLUMN` (optional, default `emailaddress`)
- `REDSHIFT_USER`
- `REDSHIFT_PASSWORD`
- `REDSHIFT_SSLMODE`
- `REDSHIFT_QUERY_CHUNK_SIZE`

### FTP or SFTP

- `FTP_PROTOCOL`
- `FTP_HOST`
- `FTP_PORT`
- `FTP_USER`
- `FTP_PASSWORD`
- `FTP_REMOTE_PATH`
- `FTP_TIMEOUT_SECONDS`

### Firehose and batching

- `BATCH_SIZE`
- `REQUEST_FILE_PREFIX`
- `REQUEST_OUTPUT_DIR`

## Local Output

Generated Email Verification Request files are written to the configured `REQUEST_OUTPUT_DIR` before upload.

## Input Format Detection

Firehose source input format is inferred per object from key extension and payload content.

- Extensions supported: `.csv`, `.json`, `.ndjson`, `.txt`, `.text`, `.log`
- Content fallback supports CSV, JSON (array/NDJSON), and plain text email lines

## Operator Validation Samples

Use these samples to validate parser behavior quickly.

### Sample SQS Event Body

```json
{
	"Records": [
		{
			"eventVersion": "2.1",
			"eventSource": "aws:s3",
			"awsRegion": "us-east-1",
			"eventTime": "2026-09-14T12:04:48.000Z",
			"eventName": "ObjectCreated:Put",
			"s3": {
				"s3SchemaVersion": "1.0",
				"bucket": {
					"name": "dnb-gpd-redshift-transfer-dev"
				},
				"object": {
					"key": "emails/2026/09/14/11/dnb-gpd-email-stream-dev-1-2026-09-14-11-04-47.txt",
					"size": 1889
				}
			}
		}
	]
}
```

### Sample Payload: CSV

```csv
email
alice@example.com
bob@example.com
Alice@example.com
not-an-email
```

### Sample Payload: NDJSON

```json
{"email":"alice@example.com"}
{"email":"bob@example.com"}
{"email":"Alice@example.com"}
{"email":"bad-value"}
```

### Sample Payload: Plain Text

```text
alice@example.com
bob@example.com; carol@example.com
Alice@example.com | dave@example.com
not-an-email
```

Expected behavior for all samples:

- emails are normalized to lowercase
- invalid email values are counted as invalid records
- duplicates are removed during in-memory deduplication

## Entry Point

The application entry point is [cmd/main.go](cmd/main.go).

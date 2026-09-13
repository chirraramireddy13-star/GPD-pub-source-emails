# GPD

GPD is a Go-based email processing pipeline that consumes SQS notifications for Firehose-delivered S3 files, extracts and filters email addresses, generates Email Verification Request batch files, and uploads those files to an FTP or SFTP destination.

See [ARCHITECTURE.md](ARCHITECTURE.md) for the end-to-end runtime flow, retry behavior, success criteria, and module responsibilities.

## Current Status

The orchestration flow is implemented in code, including:

- configuration loading and validation
- continuous SQS polling
- SQS event parsing
- S3 object download
- CSV and JSON parsing
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

External integrations are still abstracted behind injectable functions. Real production implementations still need to be wired for:

- AWS SQS
- AWS S3
- Redshift SQL access
- FTP or SFTP transport

## Project Flow

1. Load and validate environment configuration.
2. Initialize logger and service clients.
3. Poll SQS continuously with long polling.
4. Parse the SQS payload and extract S3 bucket and object key.
5. Download the Firehose batch file from S3.
6. Parse records and extract normalized valid emails.
7. Deduplicate emails in memory.
8. Query Redshift in chunks to find already-known emails.
9. Filter existing emails out.
10. Split new emails into verification request batches.
11. Create request batch files on disk.
12. Upload each batch file to FTP or SFTP.
13. Delete the SQS message after the full success path completes.
14. Log processing metrics.
15. Treat the message as successful only after all required uploads and SQS deletion succeed.

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

## Entry Point

The application entry point is [cmd/main.go](cmd/main.go).

package database

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"gpd/config"
)

type RedshiftConnection struct {
	Host             string
	Port             int
	Database         string
	User             string
	SSLMode          string
	DSN              string
	pool             *pgxpool.Pool
	emailLookupQuery EmailLookupQuery
}

type EmailLookupQuery func(ctx context.Context, query string, args []any) ([]string, error)

func NewRedshiftConnection(cfg config.RedshiftConfig) (*RedshiftConnection, error) {
	if cfg.Host == "" {
		return nil, fmt.Errorf("REDSHIFT_HOST is required")
	}
	if cfg.Port <= 0 {
		return nil, fmt.Errorf("invalid REDSHIFT_PORT: %d", cfg.Port)
	}
	if cfg.Database == "" {
		return nil, fmt.Errorf("REDSHIFT_DATABASE is required")
	}
	if cfg.User == "" {
		return nil, fmt.Errorf("REDSHIFT_USER is required")
	}
	if cfg.Password == "" {
		return nil, fmt.Errorf("REDSHIFT_PASSWORD is required")
	}
	if cfg.SSLMode == "" {
		cfg.SSLMode = "require"
	}

	dsn := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?sslmode=%s",
		url.QueryEscape(cfg.User),
		url.QueryEscape(cfg.Password),
		cfg.Host,
		cfg.Port,
		cfg.Database,
		cfg.SSLMode,
	)

	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		return nil, fmt.Errorf("connect to redshift: %w", err)
	}

	return &RedshiftConnection{
		Host:             cfg.Host,
		Port:             cfg.Port,
		Database:         cfg.Database,
		User:             cfg.User,
		SSLMode:          cfg.SSLMode,
		DSN:              dsn,
		pool:             pool,
		emailLookupQuery: newEmailLookupQuery(pool),
	}, nil
}

func (c *RedshiftConnection) SetEmailLookupQuery(queryFn EmailLookupQuery) {
	if queryFn == nil {
		c.emailLookupQuery = defaultEmailLookupQuery
		return
	}
	c.emailLookupQuery = queryFn
}

func (c *RedshiftConnection) ExistingEmails(ctx context.Context, emails []string, chunkSize int) (map[string]struct{}, error) {
	if chunkSize <= 0 {
		return nil, fmt.Errorf("chunk size must be greater than zero")
	}
	if len(emails) == 0 {
		return map[string]struct{}{}, nil
	}
	if c.emailLookupQuery == nil {
		c.emailLookupQuery = defaultEmailLookupQuery
	}

	existing := make(map[string]struct{})
	for start := 0; start < len(emails); start += chunkSize {
		end := start + chunkSize
		if end > len(emails) {
			end = len(emails)
		}

		chunk := emails[start:end]
		query, args := buildEmailUniqueQuery(chunk)

		chunkMatches, err := c.emailLookupQuery(ctx, query, args)
		if err != nil {
			return nil, fmt.Errorf("lookup existing emails for chunk [%d:%d]: %w", start, end, err)
		}

		mergeLookupSet(existing, BuildLookupSetFromResults(chunkMatches))
	}

	return existing, nil
}

func BuildLookupSetFromResults(results []string) map[string]struct{} {
	lookup := make(map[string]struct{}, len(results))
	for _, addr := range results {
		normalized := normalizeEmailValue(addr)
		if normalized == "" {
			continue
		}
		lookup[normalized] = struct{}{}
	}

	return lookup
}

func mergeLookupSet(target map[string]struct{}, source map[string]struct{}) {
	for addr := range source {
		target[addr] = struct{}{}
	}
}

func buildEmailUniqueQuery(chunk []string) (string, []any) {
	placeholders := make([]string, 0, len(chunk))
	args := make([]any, 0, len(chunk))
	for i, addr := range chunk {
		placeholders = append(placeholders, "$"+strconv.Itoa(i+1))
		args = append(args, normalizeEmailValue(addr))
	}

	query := fmt.Sprintf(
		"SELECT email FROM EmailUnique WHERE lower(trim(email)) IN (%s)",
		strings.Join(placeholders, ", "),
	)

	return query, args
}

func normalizeEmailValue(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func defaultEmailLookupQuery(_ context.Context, _ string, _ []any) ([]string, error) {
	return nil, fmt.Errorf("redshift email lookup query is not configured")
}

func newEmailLookupQuery(pool *pgxpool.Pool) EmailLookupQuery {
	return func(ctx context.Context, query string, args []any) ([]string, error) {
		rows, err := pool.Query(ctx, query, args...)
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		results := make([]string, 0)
		for rows.Next() {
			var email string
			if err := rows.Scan(&email); err != nil {
				return nil, err
			}
			results = append(results, email)
		}

		if err := rows.Err(); err != nil {
			return nil, err
		}

		return results, nil
	}
}

func (c *RedshiftConnection) Close() {
	if c.pool != nil {
		c.pool.Close()
	}
}

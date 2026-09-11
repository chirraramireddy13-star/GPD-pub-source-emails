package logger

import (
	"fmt"
	"log"
	"os"
	"strings"

	"gpd/config"
)

func Init(cfg config.LoggingConfig) (*log.Logger, error) {
	level := strings.ToUpper(strings.TrimSpace(cfg.Level))
	if level == "" {
		level = "INFO"
	}

	switch level {
	case "DEBUG", "INFO", "WARN", "ERROR":
	default:
		return nil, fmt.Errorf("invalid LOG_LEVEL: %s", cfg.Level)
	}

	format := strings.ToLower(strings.TrimSpace(cfg.Format))
	if format == "" {
		format = "text"
	}
	if format != "text" && format != "json" {
		return nil, fmt.Errorf("invalid LOG_FORMAT: %s", cfg.Format)
	}

	prefix := fmt.Sprintf("[%s] ", level)
	if format == "json" {
		prefix = fmt.Sprintf("{\"level\":\"%s\",\"msg\":", strings.ToLower(level))
	}

	return log.New(os.Stdout, prefix, log.LstdFlags|log.LUTC), nil
}

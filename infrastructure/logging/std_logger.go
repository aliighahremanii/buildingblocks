package logging

import (
	"context"
	"fmt"
	"log"

	abstractions "github.com/aliighahremanii/buildingblocks/application/abstractions"
)

// StdLogger writes structured log lines to the standard library logger.
type StdLogger struct{}

func NewStdLogger() *StdLogger { return &StdLogger{} }

func (*StdLogger) Debug(_ context.Context, msg string, fields ...abstractions.Field) {
	log.Printf("[DEBUG] %s%s", msg, formatFields(fields))
}

func (*StdLogger) Info(_ context.Context, msg string, fields ...abstractions.Field) {
	log.Printf("[INFO]  %s%s", msg, formatFields(fields))
}

func (*StdLogger) Warn(_ context.Context, msg string, fields ...abstractions.Field) {
	log.Printf("[WARN]  %s%s", msg, formatFields(fields))
}

func (*StdLogger) Error(_ context.Context, msg string, fields ...abstractions.Field) {
	log.Printf("[ERROR] %s%s", msg, formatFields(fields))
}

func formatFields(fields []abstractions.Field) string {
	if len(fields) == 0 {
		return ""
	}
	out := ""
	for _, f := range fields {
		out += fmt.Sprintf(" %s=%v", f.Key, f.Value)
	}
	return out
}

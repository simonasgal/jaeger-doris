package internal

import (
	"context"

	"go.uber.org/zap"
)

type loggerKey struct{}

var k loggerKey

func LoggerWithContext(ctx context.Context, logger *zap.Logger) context.Context {
	return context.WithValue(ctx, k, logger)
}

func LoggerFromContext(ctx context.Context) *zap.Logger {
	logger, _ := ctx.Value(k).(*zap.Logger)
	return logger
}

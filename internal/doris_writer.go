package internal

import (
	"context"
	"errors"

	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"go.uber.org/zap"
)

var (
	_ spanstore.Writer = (*dorisWriterNoop)(nil)
)

type dorisWriterNoop struct {
	logger *zap.Logger
}

func (dw *dorisWriterNoop) WriteSpan(ctx context.Context, span *model.Span) error {
	dw.logger.Debug("no-op WriteSpan called")
	return errors.New("WriteSpan is not implemented in this context")
}

package internal

import (
	"context"
	"database/sql"
	"time"

	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/dependencystore"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"go.uber.org/zap"
)

var (
	_ spanstore.Reader       = (*dorisReader)(nil)
	_ dependencystore.Reader = (*dorisDependencyReader)(nil)
)

type dorisReader struct {
	logger *zap.Logger
	db     *sql.DB
	cfg    *Config
}

func (dr *dorisReader) GetTrace(ctx context.Context, traceID model.TraceID) (*model.Trace, error) {
	trace := &model.Trace{
		Spans: make([]*model.Span, 0),
	}

	f := func(ctx context.Context, cfg *Config, record map[string]string) error {
		span, err := recordToSpan(ctx, cfg, record)
		if err != nil {
			dr.logger.Warn("Failed to convert record to span", zap.Error(err))
		} else {
			trace.Spans = append(trace.Spans, span)
		}

		return nil
	}

	err := executeQuery(ctx, dr.db, dr.cfg, queryGetTrace("otel2.traces", traceID.String()), f) // TODO: table name
	if err != nil {
		return nil, err
	}

	if len(trace.Spans) == 0 {
		return nil, spanstore.ErrTraceNotFound
	}

	return trace, nil
}

func (dr *dorisReader) GetServices(ctx context.Context) ([]string, error) {
	services := make([]string, 0)

	f := func(ctx context.Context, cfg *Config, record map[string]string) error {
		serviceName := record[SpanProcessAttributeServiceName]
		if serviceName != "" {
			services = append(services, serviceName)
		}
		return nil
	}

	err := executeQuery(ctx, dr.db, dr.cfg, queryGetServices("otel2.traces"), f) // TODO: table name
	if err != nil {
		return nil, err
	}

	return services, nil
}

func (dr *dorisReader) GetOperations(ctx context.Context, query spanstore.OperationQueryParameters) ([]spanstore.Operation, error) {
	operations := make([]spanstore.Operation, 0)

	f := func(ctx context.Context, cfg *Config, record map[string]string) error {
		operationName := record[SpanAttributeOperationName]
		spanKind := record[SpanTagAttributeSpanKind]
		if operationName != "" {
			operations = append(operations, spanstore.Operation{
				Name:     operationName,
				SpanKind: otelToJeagerSpanKind[spanKind],
			})
		}
		return nil
	}

	err := executeQuery(ctx, dr.db, dr.cfg, queryGetOperations("otel2.traces", query), f) // TODO: table name
	if err != nil {
		return nil, err
	}

	return operations, nil
}

func (dr *dorisReader) FindTraces(ctx context.Context, query *spanstore.TraceQueryParameters) ([]*model.Trace, error) {
	// TODO
	return nil, nil
}

func (dr *dorisReader) FindTraceIDs(ctx context.Context, query *spanstore.TraceQueryParameters) ([]model.TraceID, error) {
	// TODO
	return nil, nil
}

type dorisDependencyReader struct {
	logger *zap.Logger
	dr     *dorisReader
}

func (ddr *dorisDependencyReader) GetDependencies(ctx context.Context, endTs time.Time, lookback time.Duration) ([]model.DependencyLink, error) {
	// TODO
	return nil, nil
}

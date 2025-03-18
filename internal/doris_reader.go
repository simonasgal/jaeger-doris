package internal

import (
	"context"
	"database/sql"
	"fmt"
	_ "google.golang.org/grpc/encoding/gzip"
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
	schema := dr.cfg.Doris.SchemaMapping

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

	err := executeQuery(ctx, dr.db, dr.cfg, queryGetTrace(schema, dr.cfg.Doris.TableFullName(), traceIDToString(traceID)), f)
	if err != nil {
		return nil, err
	}

	if len(trace.Spans) == 0 {
		return nil, spanstore.ErrTraceNotFound
	}

	return trace, nil
}

func (dr *dorisReader) GetServices(ctx context.Context) ([]string, error) {
	schema := dr.cfg.Doris.SchemaMapping

	services := make([]string, 0)

	f := func(ctx context.Context, cfg *Config, record map[string]string) error {
		serviceName := record[schema.ServiceName]
		if serviceName != "" {
			services = append(services, serviceName)
		}
		return nil
	}

	err := executeQuery(ctx, dr.db, dr.cfg, queryGetServices(schema, dr.cfg.Doris.TableFullName()), f)
	if err != nil {
		return nil, err
	}

	return services, nil
}

func (dr *dorisReader) GetOperations(ctx context.Context, query spanstore.OperationQueryParameters) ([]spanstore.Operation, error) {
	schema := dr.cfg.Doris.SchemaMapping

	operations := make([]spanstore.Operation, 0)

	f := func(ctx context.Context, cfg *Config, record map[string]string) error {
		operationName := record[schema.SpanName]
		spanKind := record[schema.SpanKind]
		if operationName != "" {
			operations = append(operations, spanstore.Operation{
				Name:     operationName,
				SpanKind: otelToJeagerSpanKind[spanKind],
			})
		}
		return nil
	}

	err := executeQuery(ctx, dr.db, dr.cfg, queryGetOperations(schema, dr.cfg.Doris.TableFullName(), query), f)
	if err != nil {
		return nil, err
	}

	return operations, nil
}

func (dr *dorisReader) FindTraces(ctx context.Context, query *spanstore.TraceQueryParameters) ([]*model.Trace, error) {
	schema := dr.cfg.Doris.SchemaMapping

	traceIDs := make([]string, 0)

	f := func(ctx context.Context, cfg *Config, record map[string]string) error {
		traceID, ok := record[schema.TraceID]
		if !ok || traceID == "" {
			return fmt.Errorf("invalid trace_id")
		}
		traceIDs = append(traceIDs, traceID)
		return nil
	}

	err := executeQuery(ctx, dr.db, dr.cfg, queryFindTraceIDs(schema, dr.cfg.Doris.TableFullName(), query, dr.cfg.Doris.Location), f)
	if err != nil {
		return nil, err
	}

	traces := make([]*model.Trace, 0, len(traceIDs))
	if len(traceIDs) == 0 {
		return traces, nil
	}

	traceMap := make(map[string]*model.Trace)
	for _, traceID := range traceIDs {
		traceMap[traceID] = &model.Trace{
			Spans: make([]*model.Span, 0),
		}
	}

	f = func(ctx context.Context, cfg *Config, record map[string]string) error {
		span, err := recordToSpan(ctx, cfg, record)
		if err != nil {
			dr.logger.Warn("Failed to convert record to span", zap.Error(err))
		} else {
			traceIDString := record[schema.TraceID]
			traceMap[traceIDString].Spans = append(traceMap[traceIDString].Spans, span)
		}

		return nil
	}

	err = executeQuery(ctx, dr.db, dr.cfg, queryFindTraces(schema, dr.cfg.Doris.TableFullName(), traceIDs), f)
	if err != nil {
		return nil, err
	}

	for _, trace := range traceMap {
		if len(trace.Spans) > 0 {
			traces = append(traces, trace)
			for _, s := range trace.Spans {
				for _, t := range s.Tags {
					if len(t.VStr) > 1000000 {
						fmt.Printf("==== ultra large tag detected, spanID: %v\n", s.SpanID)
					}
				}
			}
		}
	}

	return traces, nil
}

func (dr *dorisReader) FindTraceIDs(ctx context.Context, query *spanstore.TraceQueryParameters) ([]model.TraceID, error) {
	schema := dr.cfg.Doris.SchemaMapping

	traceIDs := make([]model.TraceID, 0)

	f := func(ctx context.Context, cfg *Config, record map[string]string) error {
		traceID, err := model.TraceIDFromString(record[schema.TraceID])
		if err != nil {
			return err
		}
		traceIDs = append(traceIDs, traceID)
		return nil
	}

	err := executeQuery(ctx, dr.db, dr.cfg, queryFindTraceIDs(schema, dr.cfg.Doris.TableFullName(), query, dr.cfg.Doris.Location), f)
	if err != nil {
		return nil, err
	}

	return traceIDs, nil
}

type dorisDependencyReader struct {
	logger *zap.Logger
	dr     *dorisReader
}

func (ddr *dorisDependencyReader) GetDependencies(ctx context.Context, endTs time.Time, lookback time.Duration) ([]model.DependencyLink, error) {
	graphSchema := ddr.dr.cfg.Doris.GraphSchemaMapping

	var links []model.DependencyLink

	f := func(ctx context.Context, cfg *Config, record map[string]string) error {
		link, err := recordToDependencyLink(ctx, cfg, record)
		if err != nil {
			ddr.logger.Warn("Failed to convert record to dependency link", zap.Error(err))
		} else {
			links = append(links, *link)
		}

		return nil
	}

	err := executeQuery(ctx, ddr.dr.db, ddr.dr.cfg, queryGetDependencies(graphSchema, ddr.dr.cfg.Doris.GraphTableFullName(), endTs, lookback, ddr.dr.cfg.Doris.Location), f)
	if err != nil {
		return nil, err
	}

	return links, nil
}

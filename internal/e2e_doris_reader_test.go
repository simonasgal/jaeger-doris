package internal

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

const dsn = "admin:admin@tcp(127.0.0.1:9030)/otel"

func getTestReader(ctx context.Context) (*dorisReader, error) {
	conn, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	err = conn.Ping()
	if err != nil {
		return nil, err
	}

	return &dorisReader{
		logger: LoggerFromContext(ctx),
		db:     conn,
		cfg:    &Config{},
	}, nil
}

func initContext() (context.Context, error) {
	ctx := context.Background()
	logger, err := zap.NewDevelopment()
	if err != nil {
		return nil, err
	}
	ctx = LoggerWithContext(ctx, logger)

	return ctx, nil
}

func TestGetTrace(t *testing.T) {
	ctx, err := initContext()
	require.NoError(t, err)

	dorisReader, err := getTestReader(ctx)
	require.NoError(t, err)

	traceID, err := model.TraceIDFromString("01020301000000000000000000000000")
	require.NoError(t, err)

	trace, err := dorisReader.GetTrace(ctx, traceID)
	require.NoError(t, err)
	require.Equal(t, 1, len(trace.Spans))
}

func TestGetServices(t *testing.T) {
	ctx, err := initContext()
	require.NoError(t, err)

	dorisReader, err := getTestReader(ctx)
	require.NoError(t, err)

	services, err := dorisReader.GetServices(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(services))
	require.Equal(t, "test-service", services[0])
}

func TestGetOperations(t *testing.T) {
	ctx, err := initContext()
	require.NoError(t, err)

	dorisReader, err := getTestReader(ctx)
	require.NoError(t, err)

	operations, err := dorisReader.GetOperations(ctx, spanstore.OperationQueryParameters{
		ServiceName: "test-service",
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(operations))
	require.Equal(t, "call db", operations[0].Name)
	require.Equal(t, "internal", operations[0].SpanKind)
}

var ts = time.Date(2024, 1, 1, 1, 1, 1, 1000, time.Local)
var param = &spanstore.TraceQueryParameters{
	ServiceName:   "service_name",
	OperationName: "",
	Tags: map[string]string{
		"a": "1",
		"k": "v",
	},
	StartTimeMin: ts,
	StartTimeMax: ts.Add(300 * 24 * time.Hour),
	DurationMin:  time.Microsecond,
	DurationMax:  time.Minute,
	NumTraces:    10,
}

func TestFindTraces(t *testing.T) {
	ctx, err := initContext()
	require.NoError(t, err)

	dorisReader, err := getTestReader(ctx)
	require.NoError(t, err)

	traces, err := dorisReader.FindTraces(ctx, param)
	require.NoError(t, err)
	require.Equal(t, 2, len(traces))
	require.Equal(t, 2, len(traces[0].Spans))
	require.Equal(t, 2, len(traces[1].Spans))
}

func TestFindTraceIDs(t *testing.T) {
	ctx, err := initContext()
	require.NoError(t, err)

	dorisReader, err := getTestReader(ctx)
	require.NoError(t, err)

	traces, err := dorisReader.FindTraceIDs(ctx, param)
	require.NoError(t, err)
	require.Equal(t, 2, len(traces))
}

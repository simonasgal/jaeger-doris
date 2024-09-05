package internal

import (
	"testing"

	"github.com/jaegertracing/jaeger/storage/spanstore"
	"github.com/stretchr/testify/require"
)

func TestQueryGetTrace(t *testing.T) {
	tableName := "otel2.traces"
	traceID := "01020301000000000000000000000000"
	want := `SELECT * FROM otel2.traces WHERE trace_id = "01020301000000000000000000000000"`
	require.Equal(t, want, queryGetTrace(tableName, traceID))
}

func TestQueryGetServices(t *testing.T) {
	tableName := "otel2.traces"
	want := `SELECT service_name FROM otel2.traces GROUP BY service_name`
	require.Equal(t, want, queryGetServices(tableName))
}

func TestQueryGetOperations(t *testing.T) {
	tableName := "otel2.traces"
	param := spanstore.OperationQueryParameters{
		ServiceName: "test-service",
	}
	want := `SELECT span_name, span_kind FROM otel2.traces WHERE service_name = "test-service" GROUP BY span_name, span_kind`
	require.Equal(t, want, queryGetOperations(tableName, param))

	param.SpanKind = "internal"
	want = `SELECT span_name, span_kind FROM otel2.traces WHERE service_name = "test-service" AND span_kind = "SPAN_KIND_INTERNAL" GROUP BY span_name, span_kind`
	require.Equal(t, want, queryGetOperations(tableName, param))
}


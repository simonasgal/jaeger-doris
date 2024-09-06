package internal

import (
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

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

func TestQueryFindTraces(t *testing.T) {
	tableName := "otel2.traces"
	traceIDs := []string{"01020301000000000000000000000000", "01020301000000000000000000000001"}
	want := `SELECT * FROM otel2.traces WHERE trace_id IN ('01020301000000000000000000000000','01020301000000000000000000000001')`
	require.Equal(t, want, queryFindTraces(tableName, traceIDs))
}

func TestQueryFindTraceIDs(t *testing.T) {
	ts := time.Date(2024, 1, 1, 1, 1, 1, 1000, time.Local)
	tableName := "otel2.traces"
	param := &spanstore.TraceQueryParameters{
		ServiceName:   "test-service",
		OperationName: "test-operation",
		Tags: map[string]string{
			"key1": "value1",
			"key2": "value2",
		},
		StartTimeMin: ts,
		StartTimeMax: ts.Add(time.Hour),
		DurationMin:  time.Second,
		DurationMax:  time.Minute,
		NumTraces:    10,
	}

	first := `SELECT trace_id, MIN(timestamp) AS t FROM otel2.traces WHERE `
	middle_list := []string{
		"service_name = 'test-service'",
		"span_name = 'test-operation'",
		"span_attributes['key1'] = 'value1'",
		"span_attributes['key2'] = 'value2'",
		"timestamp >= '2024-01-01 01:01:01.000001'",
		"timestamp <= '2024-01-01 02:01:01.000001'",
		"duration >= 1000000",
		"duration <= 60000000",
	}
	sort.Strings(middle_list)
	last := ` GROUP BY trace_id ORDER BY t DESC LIMIT 10`

	realQuery := queryFindTraceIDs(tableName, param, time.Local)
	fmt.Println(realQuery)
	require.Equal(t, first, realQuery[:len(first)])
	require.Equal(t, last, realQuery[len(realQuery)-len(last):])

	middle := strings.Split(realQuery[len(first):len(realQuery)-len(last)], " AND ")
	sort.Strings(middle)
	require.Equal(t, middle_list, middle)
}

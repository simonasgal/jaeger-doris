package internal

import (
	"fmt"

	"github.com/jaegertracing/jaeger/storage/spanstore"
)

func queryGetTrace(tableName string, traceID string) string {
	return fmt.Sprintf(
		`SELECT * FROM %s WHERE %s = "%s"`,
		tableName,
		SpanAttributeTraceID,
		traceID,
	)
}

func queryGetServices(tableName string) string {
	return fmt.Sprintf(
		`SELECT %s FROM %s GROUP BY %s`,
		SpanProcessAttributeServiceName,
		tableName,
		SpanProcessAttributeServiceName,
	)
}

func queryGetOperations(tableName string, param spanstore.OperationQueryParameters) string {
	query := fmt.Sprintf(
		`SELECT %s, %s FROM %s WHERE %s = "%s"`,
		SpanAttributeOperationName,
		SpanTagAttributeSpanKind,
		tableName,
		SpanProcessAttributeServiceName,
		param.ServiceName,
	)

	if param.SpanKind != "" {
		query += fmt.Sprintf(
			` AND %s = "%s"`,
			SpanTagAttributeSpanKind,
			jeagerToOtelSpanKind[param.SpanKind],
		)
	}

	query += fmt.Sprintf(
		` GROUP BY %s, %s`,
		SpanAttributeOperationName,
		SpanTagAttributeSpanKind,
	)

	return query
}

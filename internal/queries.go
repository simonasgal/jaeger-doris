package internal

import (
	"fmt"
	"strings"
	"time"

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

func queryFindTraces(tableName string, traceIDs []string) string {
	for i, traceID := range traceIDs {
		traceIDs[i] = fmt.Sprintf(`'%s'`, traceID)
	}
	traceIDsString := strings.Join(traceIDs, ",")

	return fmt.Sprintf(
		`SELECT * FROM %s WHERE %s IN (%s)`,
		tableName,
		SpanAttributeTraceID,
		traceIDsString,
	)
}

func queryFindTraceIDs(tableName string, param *spanstore.TraceQueryParameters) string {
	tags := make(map[string]string, len(param.Tags))
	for k, v := range param.Tags {
		tags[k] = v
	}

	predicates := make([]string, 0, len(tags)+6)
	for k, v := range tags {
		predicates = append(predicates, fmt.Sprintf(
			`%s['%s'] = '%s'`,
			SpanAttributeTags,
			k,
			v,
		))
	}

	if param.ServiceName != "" {
		predicates = append(predicates, fmt.Sprintf(
			`%s = '%s'`,
			SpanProcessAttributeServiceName,
			param.ServiceName,
		))
	}

	if param.OperationName != "" {
		predicates = append(predicates, fmt.Sprintf(
			`%s = '%s'`,
			SpanAttributeOperationName,
			param.OperationName,
		))
	}

	if !param.StartTimeMin.IsZero() {
		predicates = append(predicates, fmt.Sprintf(
			`%s >= '%s'`,
			SpanAttributeStartTime,
			param.StartTimeMin.In(time.Local).Format(timeFormat), // TODO timezone
		))
	}

	if !param.StartTimeMax.IsZero() {
		predicates = append(predicates, fmt.Sprintf(
			`%s <= '%s'`,
			SpanAttributeStartTime,
			param.StartTimeMax.In(time.Local).Format(timeFormat), // TODO timezone
		))
	}

	if param.DurationMin > 0 {
		predicates = append(predicates,
			fmt.Sprintf(
				`%s >= %d`,
				SpanAttributeDuration,
				param.DurationMin.Microseconds(),
			))
	}

	if param.DurationMax > 0 {
		predicates = append(predicates,
			fmt.Sprintf(
				`%s <= %d`,
				SpanAttributeDuration,
				param.DurationMax.Microseconds(),
			))
	}

	query := fmt.Sprintf(
		`SELECT %s, MIN(%s) AS t FROM %s`,
		SpanAttributeTraceID,
		SpanAttributeStartTime,
		tableName,
	)

	if len(predicates) > 0 {
		query += fmt.Sprintf(
			" WHERE %s",
			strings.Join(predicates, " AND "),
		)
	}

	query += fmt.Sprintf(
		` GROUP BY %s ORDER BY t DESC LIMIT %d`,
		SpanAttributeTraceID,
		param.NumTraces,
	)

	return query
}

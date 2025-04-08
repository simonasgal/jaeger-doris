package internal

import (
	"fmt"
	"strings"
	"time"

	"github.com/jaegertracing/jaeger/storage/spanstore"
)

func queryGetTrace(schema *SchemaMapping, tableName string, traceID string) string {
	return fmt.Sprintf(
		`SELECT * FROM %s WHERE %s = "%s"`,
		tableName,
		schema.TraceID,
		traceID,
	)
}

func queryGetServices(schema *SchemaMapping, tableName string) string {
	return fmt.Sprintf(
		`SELECT %s FROM %s GROUP BY %s`,
		schema.ServiceName,
		tableName,
		schema.ServiceName,
	)
}

func queryGetOperations(schema *SchemaMapping, tableName string, param spanstore.OperationQueryParameters) string {
	query := fmt.Sprintf(
		`SELECT %s, %s FROM %s WHERE %s = "%s"`,
		schema.SpanName,
		schema.SpanKind,
		tableName,
		schema.ServiceName,
		param.ServiceName,
	)

	if param.SpanKind != "" {
		query += fmt.Sprintf(
			` AND %s = "%s"`,
			schema.SpanKind,
			jeagerToOtelSpanKind[param.SpanKind],
		)
	}

	query += fmt.Sprintf(
		` GROUP BY %s, %s`,
		schema.SpanName,
		schema.SpanKind,
	)

	return query
}

func queryFindTraces(schema *SchemaMapping, tableName string, traceIDs []string, partitionTS time.Time) string {
	predicates := make([]string, 0, 2)

	// add partitions select timestamp to reduce partitions to scan
	// truncate to hour boundaries and take back one hour just to be sure we're selecting not too small window.
	partitionTS = partitionTS.Truncate(time.Hour).Add(-time.Hour)
	if !partitionTS.IsZero() {
		predicates = append(predicates,
			fmt.Sprintf(`(%s >= "%s")`,
				schema.Timestamp,
				partitionTS.Format(timestampLayout)),
		)
	}
	for i, traceID := range traceIDs {
		traceIDs[i] = fmt.Sprintf(`'%s'`, traceID)
	}
	traceIDsString := strings.Join(traceIDs, ",")
	predicates = append(predicates, fmt.Sprintf(`%s IN (%s)`, schema.TraceID, traceIDsString))

	return fmt.Sprintf(
		`SELECT * FROM %s WHERE %s`,
		tableName,
		strings.Join(predicates, " AND "),
	)
}

func queryFindTraceIDs(schema *SchemaMapping, tableName string, param *spanstore.TraceQueryParameters, location *time.Location) string {
	tags := make(map[string]string, len(param.Tags))
	for k, v := range param.Tags {
		tags[k] = v
	}

	predicates := make([]string, 0, len(tags)+6)
	for k, v := range tags {
		// XXX: work around special case: "error=true" must be treated separately
		// since there is no such tag "error", instead there is
		// string value "error.msg".
		//
		// Drop this after "error" tag is treated correctly at the ingestion time
		// in otelcol-contrib by the doris exporter
		if k == "error" && v == "true" {
			predicates = append(predicates,
				fmt.Sprintf(
					`((%s['error.msg'] IS NOT NULL) OR (%s == "STATUS_CODE_ERROR"))`,
					schema.SpanAttributes,
					schema.StatusCode,
				))

		} else {
			predicates = append(predicates,
				fmt.Sprintf(
					`%s['%s'] = '%s'`,
					schema.SpanAttributes,
					k,
					v,
				))
		}
	}

	if param.ServiceName != "" {
		predicates = append(predicates, fmt.Sprintf(
			`%s = '%s'`,
			schema.ServiceName,
			param.ServiceName,
		))
	}

	if param.OperationName != "" {
		predicates = append(predicates, fmt.Sprintf(
			`%s = '%s'`,
			schema.SpanName,
			param.OperationName,
		))
	}

	if !param.StartTimeMin.IsZero() {
		predicates = append(predicates, fmt.Sprintf(
			`%s >= '%s'`,
			schema.Timestamp,
			param.StartTimeMin.In(location).Format(timeFormat),
		))
	}

	if !param.StartTimeMax.IsZero() {
		predicates = append(predicates, fmt.Sprintf(
			`%s <= '%s'`,
			schema.Timestamp,
			param.StartTimeMax.In(location).Format(timeFormat),
		))
	}

	if param.DurationMin > 0 {
		predicates = append(predicates,
			fmt.Sprintf(
				`%s >= %d`,
				schema.Duration,
				param.DurationMin.Microseconds(),
			))
	}

	if param.DurationMax > 0 {
		predicates = append(predicates,
			fmt.Sprintf(
				`%s <= %d`,
				schema.Duration,
				param.DurationMax.Microseconds(),
			))
	}

	query := fmt.Sprintf(
		`SELECT %s, MIN(%s) AS t FROM %s`,
		schema.TraceID,
		schema.Timestamp,
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
		schema.TraceID,
		param.NumTraces,
	)

	return query
}

func queryGetDependencies(graphSchema *GraphSchemaMapping, tableName string, endTs time.Time, lookback time.Duration, location *time.Location) string {
	template := `select
%s, %s, sum(%s) as %s
from %s
where timestamp >= '%s'
and timestamp <= '%s'
group by %s, %s`

	return fmt.Sprintf(
		template,
		graphSchema.CallerServiceName,
		graphSchema.CalleeServiceName,
		graphSchema.Count, graphSchema.Count,
		tableName,
		endTs.Add(-lookback).In(location).Format(timeFormat),
		endTs.In(location).Format(timeFormat),
		graphSchema.CallerServiceName,
		graphSchema.CalleeServiceName,
	)
}

package internal

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jaegertracing/jaeger/model"
	"github.com/opentracing/opentracing-go/ext"

	"go.uber.org/zap"
)

const timeFormat = "2006-01-02 15:04:05.999999"

func traceIDToString(traceID model.TraceID) string {
	// model.TraceID.String() does not convert the high portion if it is zero
	return fmt.Sprintf("%016x%016x", traceID.High, traceID.Low)
}

type mappingFunc func(ctx context.Context, cfg *Config, record map[string]string) error

func executeQuery(ctx context.Context, db *sql.DB, cfg *Config, query string, f mappingFunc) error {
	if cfg.Service.TimeoutSecond > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(cfg.Service.TimeoutSecond)*time.Second)
		defer cancel()
	}

	logger := LoggerFromContext(ctx)

	logger.Debug("executing query", zap.String("query", query))

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	m := make(map[string]string, len(columns))

	cache := make([]any, len(columns))
	for i := range cache {
		cache[i] = new(any)
	}

	for rows.Next() {
		err = rows.Scan(cache...)
		if err != nil {
			return err
		}

		for i, k := range columns {
			v := cache[i].(*any)

			// the value of the map is not null
			if v == nil || *v == nil {
				delete(m, k)
			} else {
				vv, ok := (*v).([]byte)
				if !ok {
					return fmt.Errorf("invalid column %s", k)
				} else {
					m[k] = string(vv)
				}
			}
		}

		err = f(ctx, cfg, m)
		if err != nil {
			return err
		}
	}

	return err
}

type otelLink struct {
	TraceID string `json:"trace_id"`
	SpanID  string `json:"span_id"`
}

type otelEvent struct {
	Timestamp  string         `json:"timestamp"`
	Name       string         `json:"name"`
	Attributes map[string]any `json:"attributes"`
}

func recordToSpan(ctx context.Context, cfg *Config, record map[string]string) (*model.Span, error) {
	logger := LoggerFromContext(ctx)
	location := cfg.Doris.Location
	schema := cfg.Doris.SchemaMapping

	span := &model.Span{}

	// TraceID
	traceIDString, ok := record[schema.TraceID]
	if !ok {
		return nil, fmt.Errorf("invalid trace_id")
	}
	traceID, err := model.TraceIDFromString(traceIDString)
	if err != nil {
		return nil, err
	}
	span.TraceID = traceID

	// SpanID
	spanIDString, ok := record[schema.SpanID]
	if !ok {
		return nil, fmt.Errorf("invalid span_id")
	}
	spanID, err := model.SpanIDFromString(spanIDString)
	if err != nil {
		return nil, err
	}
	span.SpanID = spanID

	// OperationName
	operationName, ok := record[schema.SpanName]
	if !ok {
		return nil, fmt.Errorf("invalid span_name")
	}
	span.OperationName = operationName

	// References
	references := []model.SpanRef{}

	parentSpanIDString := record[schema.ParentSpanID]
	if parentSpanIDString != "" {
		parentSpanID, err := model.SpanIDFromString(parentSpanIDString)
		if err != nil {
			return nil, err
		}
		references = append(references, model.SpanRef{
			TraceID: traceID,
			SpanID:  parentSpanID,
			RefType: model.ChildOf,
		})
	}

	referencesFollowsFromString := record[schema.Links]
	if referencesFollowsFromString != "" {
		referencesFollowsFrom := []*otelLink{}
		err = json.Unmarshal([]byte(referencesFollowsFromString), &referencesFollowsFrom)
		if err != nil {
			logger.Warn("failed to unmarshal links", zap.Error(err))
		} else {
			for _, ref := range referencesFollowsFrom {
				traceID, err := model.TraceIDFromString(ref.TraceID)
				if err != nil {
					logger.Warn("failed to parse trace_id of reference", zap.Error(err))
					continue
				}
				spanID, err := model.SpanIDFromString(ref.SpanID)
				if err != nil {
					logger.Warn("failed to parse span_id of reference", zap.Error(err))
					continue
				}
				references = append(references, model.SpanRef{
					TraceID: traceID,
					SpanID:  spanID,
					RefType: model.FollowsFrom,
				})
			}
		}
	}
	span.References = references

	// StartTime
	startTimeString, ok := record[schema.Timestamp]
	if !ok {
		return nil, fmt.Errorf("invalid timestamp")
	}
	startTime, err := time.ParseInLocation(timeFormat, startTimeString, location)
	if err != nil {
		return nil, err
	}
	span.StartTime = startTime

	// Duration
	durationString, ok := record[schema.Duration]
	if !ok {
		return nil, fmt.Errorf("invalid duration")
	}

	duration, err := strconv.ParseInt(durationString, 10, 0)
	if err != nil {
		return nil, err
	}

	span.Duration = time.Duration(duration * 1000)

	// Tags
	tags := []model.KeyValue{}
	tagsString := record[schema.SpanAttributes]
	if tagsString != "" {
		attributes := make(map[string]any)
		err = json.Unmarshal([]byte(tagsString), &attributes)
		if err != nil {
			logger.Warn("failed to unmarshal span_attributes", zap.Error(err))
		} else {
			for k, v := range attributes {
				tags = append(tags, kvToKeyValue(k, v))
			}
		}
	}

	// Tags.SpanKind
	spanKind, ok := record[schema.SpanKind]
	if !ok {
		logger.Warn("invalid span_kind")
	} else {
		spanKind, ok := otelToJeagerSpanKind[spanKind]
		if !ok {
			logger.Warn("invalid span_kind")
		} else {
			tags = append(tags, model.String(string(ext.SpanKind), spanKind))
		}
	}

	// Tags.StatusDescription
	statusMessage, ok := record[schema.StatusMessage]
	if !ok {
		logger.Warn("invalid status_message")
	} else {
		tags = append(tags, model.String(SpanTagKeyStatusDescription, statusMessage))
	}

	// Tags.StatusCode
	statusCode, ok := record[schema.StatusCode]
	if !ok {
		logger.Warn("invalid status_code")
	} else {
		tags = append(tags, model.String(SpanTagKeyStatusCode, statusCode))
		if statusCode == StatusCodeError {
			tags = append(tags, model.Bool(SpanTagKeyError, true))
		}
	}

	span.Tags = tags

	// Logs
	logs := []model.Log{}
	logsString := record[schema.Events]
	if logsString != "" {
		events := []*otelEvent{}
		err = json.Unmarshal([]byte(logsString), &events)
		if err != nil {
			logger.Warn("failed to unmarshal events", zap.Error(err))
		} else {
			for _, event := range events {
				timestamp, err := time.ParseInLocation(timeFormat, event.Timestamp, location)
				if err != nil {
					logger.Warn("failed to parse timestamp of event", zap.Error(err))
					continue
				}
				fields := []model.KeyValue{}
				fields = append(fields, model.String(SpanLogFieldKeyEvent, event.Name))
				for k, v := range event.Attributes {
					fields = append(fields, kvToKeyValue(k, v))
				}
				logs = append(logs, model.Log{
					Timestamp: timestamp,
					Fields:    fields,
				})
			}
		}
	}
	span.Logs = logs

	// Process
	serviceName, ok := record[schema.ServiceName]
	if !ok {
		return nil, fmt.Errorf("invalid service_name")
	}

	processTags := []model.KeyValue{}
	processTagsString := record[schema.ResourceAttributes]
	if processTagsString != "" {
		attributes := make(map[string]any)
		err = json.Unmarshal([]byte(processTagsString), &attributes)
		if err != nil {
			logger.Warn("failed to unmarshal resource_attributes", zap.Error(err))
		} else {
			for k, v := range attributes {
				processTags = append(processTags, kvToKeyValue(k, v))
			}
		}
	}

	span.Process = &model.Process{
		ServiceName: serviceName,
		Tags:        processTags,
	}

	return span, nil
}

func recordToDependencyLink(_ context.Context, cfg *Config, record map[string]string) (*model.DependencyLink, error) {
	graphSchema := cfg.Doris.GraphSchemaMapping

	dependencyLink := &model.DependencyLink{}

	// Parent
	parent, ok := record[graphSchema.CallerServiceName]
	if !ok {
		return nil, fmt.Errorf("invalid client")
	}
	dependencyLink.Parent = parent

	// Child
	child, ok := record[graphSchema.CalleeServiceName]
	if !ok {
		return nil, fmt.Errorf("invalid server")
	}
	dependencyLink.Child = child

	// CallCount
	callCountString, ok := record[graphSchema.Count]
	if !ok {
		return nil, fmt.Errorf("invalid value")
	}
	callCount, err := strconv.ParseInt(callCountString, 10, 0)
	if err != nil {
		return nil, err
	}
	dependencyLink.CallCount = uint64(callCount)

	return dependencyLink, nil
}

func kvToKeyValue(k string, v any) model.KeyValue {
	switch vv := v.(type) {
	case bool:
		return model.Bool(k, vv)
	case float64:
		return model.Float64(k, vv)
	case int64:
		return model.Int64(k, vv)
	case string:
		return model.String(k, vv)
	default:
		return model.String(k, fmt.Sprint(vv))
	}
}

var otelToJeagerSpanKind map[string]string = map[string]string{
	SpanKindInternal: "internal",
	SpanKindServer:   string(ext.SpanKindRPCServerEnum),
	SpanKindClient:   string(ext.SpanKindRPCClientEnum),
	SpanKindProducer: string(ext.SpanKindProducerEnum),
	SpanKindConsumer: string(ext.SpanKindConsumerEnum),
}

var jeagerToOtelSpanKind map[string]string = map[string]string{
	"internal":                        SpanKindInternal,
	string(ext.SpanKindRPCServerEnum): SpanKindServer,
	string(ext.SpanKindRPCClientEnum): SpanKindClient,
	string(ext.SpanKindProducerEnum):  SpanKindProducer,
	string(ext.SpanKindConsumerEnum):  SpanKindConsumer,
}

const (
	// TODO reference
	SpanTagKeyStatusDescription = "otel.status_description"
	SpanTagKeyStatusCode        = "otel.status_code"
	SpanTagKeyError             = "error"

	SpanLogFieldKeyEvent = "event"

	// TODO reference
	SpanKindInternal = "SPAN_KIND_INTERNAL"
	SpanKindServer   = "SPAN_KIND_SERVER"
	SpanKindClient   = "SPAN_KIND_CLIENT"
	SpanKindProducer = "SPAN_KIND_PRODUCER"
	SpanKindConsumer = "SPAN_KIND_CONSUMER"

	// TODO reference
	StatusCodeError = "STATUS_CODE_ERROR"
)

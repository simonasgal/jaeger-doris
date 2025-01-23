package internal

import (
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	Service *ServiceConfig `yaml:"service"`
	Doris   *DorisConfig   `yaml:"doris"`
}

type ServiceConfig struct {
	IP            string `yaml:"ip" mapstructure:"ip"`
	Port          int32  `yaml:"port" mapstructure:"port"`
	LogLevel      string `yaml:"log_level" mapstructure:"log_level"`
	TimeoutSecond int64  `yaml:"timeout" mapstructure:"timeout"`
}

type DorisConfig struct {
	Endpoint           string              `yaml:"endpoint" mapstructure:"endpoint"`
	Username           string              `yaml:"username" mapstructure:"username"`
	Password           string              `yaml:"password" mapstructure:"password"`
	Database           string              `yaml:"database" mapstructure:"database"`
	Table              string              `yaml:"table" mapstructure:"table"`
	SchemaMapping      *SchemaMapping      `yaml:"schema_mapping" mapstructure:"schema_mapping"`
	GraphTable         string              `yaml:"graph_table" mapstructure:"graph_table"`
	GraphSchemaMapping *GraphSchemaMapping `yaml:"graph_schema_mapping" mapstructure:"graph_schema_mapping"`
	TimeZone           string              `yaml:"timezone" mapstructure:"timezone"` // doris does not handle time zones and needs to be handled manually

	Location *time.Location `yaml:"-"`
}

type SchemaMapping struct {
	ServiceName        string `yaml:"service_name" mapstructure:"service_name"`               // otlp doris exporter: service_name			jaeger: Span.Process.ServiceName
	Timestamp          string `yaml:"timestamp" mapstructure:"timestamp"`                     // otlp doris exporter: timestamp				jaeger: Span.StartTime
	ServiceInstanceID  string `yaml:"service_instance_id" mapstructure:"service_instance_id"` // otlp doris exporter: service_instance_id	jaeger: -
	TraceID            string `yaml:"trace_id" mapstructure:"trace_id"`                       // otlp doris exporter: trace_id				jaeger: Span.TraceID
	SpanID             string `yaml:"span_id" mapstructure:"span_id"`                         // otlp doris exporter: span_id				jaeger: Span.SpanID
	TraceState         string `yaml:"trace_state" mapstructure:"trace_state"`                 // otlp doris exporter: trace_state			jaeger: -
	ParentSpanID       string `yaml:"parent_span_id" mapstructure:"parent_span_id"`           // otlp doris exporter: parent_span_id		jaeger: Span.References[SpanRef.RefType == ChildOf].SpanID
	SpanName           string `yaml:"span_name" mapstructure:"span_name"`                     // otlp doris exporter: span_name				jaeger: Span.OperationName
	SpanKind           string `yaml:"span_kind" mapstructure:"span_kind"`                     // otlp doris exporter: span_kind				jaeger: Span.Tags[SpanKind]
	EndTime            string `yaml:"end_time" mapstructure:"end_time"`                       // otlp doris exporter: end_time				jaeger: -
	Duration           string `yaml:"duration" mapstructure:"duration"`                       // otlp doris exporter: duration				jaeger: Span.Duration
	SpanAttributes     string `yaml:"span_attributes" mapstructure:"span_attributes"`         // otlp doris exporter: span_attributes		jaager: Span.Tags
	Events             string `yaml:"events" mapstructure:"events"`                           // otlp doris exporter: events				jaeger: Span.Logs
	Links              string `yaml:"links" mapstructure:"links"`                             // otlp doris exporter: links					jaeger: Span.References[SpanRef.RefType == FollowsFrom]
	StatusMessage      string `yaml:"status_message" mapstructure:"status_message"`           // otlp doris exporter: status_message		jaeger: Span.Tags["otel.status_description"]
	StatusCode         string `yaml:"status_code" mapstructure:"status_code"`                 // otlp doris exporter: status_code			jaager: Span.Tags["otel.status_code"]
	ResourceAttributes string `yaml:"resource_attributes" mapstructure:"resource_attributes"` // otlp doris exporter: resource_attributes	jaeger: Span.Process.Tags
	ScopeName          string `yaml:"scope_name" mapstructure:"scope_name"`                   // otlp doris exporter: scope_name			jaeger: -
	ScopeVersion       string `yaml:"scope_version" mapstructure:"scope_version"`             // otlp doris exporter: scope_version			jaeger: -
}

func (s *SchemaMapping) FillDefaultValues() {
	if s.ServiceName == "" {
		s.ServiceName = "service_name"
	}
	if s.Timestamp == "" {
		s.Timestamp = "timestamp"
	}
	if s.ServiceInstanceID == "" {
		s.ServiceInstanceID = "service_instance_id"
	}
	if s.TraceID == "" {
		s.TraceID = "trace_id"
	}
	if s.SpanID == "" {
		s.SpanID = "span_id"
	}
	if s.TraceState == "" {
		s.TraceState = "trace_state"
	}
	if s.ParentSpanID == "" {
		s.ParentSpanID = "parent_span_id"
	}
	if s.SpanName == "" {
		s.SpanName = "span_name"
	}
	if s.SpanKind == "" {
		s.SpanKind = "span_kind"
	}
	if s.EndTime == "" {
		s.EndTime = "end_time"
	}
	if s.Duration == "" {
		s.Duration = "duration"
	}
	if s.SpanAttributes == "" {
		s.SpanAttributes = "span_attributes"
	}
	if s.Events == "" {
		s.Events = "events"
	}
	if s.Links == "" {
		s.Links = "links"
	}
	if s.StatusMessage == "" {
		s.StatusMessage = "status_message"
	}
	if s.StatusCode == "" {
		s.StatusCode = "status_code"
	}
	if s.ResourceAttributes == "" {
		s.ResourceAttributes = "resource_attributes"
	}
	if s.ScopeName == "" {
		s.ScopeName = "scope_name"
	}
	if s.ScopeVersion == "" {
		s.ScopeVersion = "scope_version"
	}
}

type GraphSchemaMapping struct {
	Timestamp               string `yaml:"timestamp" mapstructure:"timestamp"`                                   // otlp doris exporter: timestamp					jaeger: -
	CallerServiceName       string `yaml:"caller_service_name" mapstructure:"caller_service_name"`               // otlp doris exporter: caller_service_name		jaeger: DependencyLink.Parent
	CallerServiceInstanceID string `yaml:"caller_service_instance_id" mapstructure:"caller_service_instance_id"` // otlp doris exporter: caller_service_instance_id	jaeger: -
	CalleeServiceName       string `yaml:"callee_service_name" mapstructure:"callee_service_name"`               // otlp doris exporter: callee_service_name		jaeger: DependencyLink.Child
	CalleeServiceInstanceID string `yaml:"callee_service_instance_id" mapstructure:"callee_service_instance_id"` // otlp doris exporter: callee_service_instance_id	jaeger: -
	Count                   string `yaml:"count" mapstructure:"count"`                                           // otlp doris exporter: count						jaeger: DependencyLink.CallCount
	ErrorCount              string `yaml:"error_count" mapstructure:"error_count"`                               // otlp doris exporter: error_count				jaeger: -
}

func (s *GraphSchemaMapping) FillDefaultValues() {
	if s.Timestamp == "" {
		s.Timestamp = "timestamp"
	}
	if s.CallerServiceName == "" {
		s.CallerServiceName = "caller_service_name"
	}
	if s.CallerServiceInstanceID == "" {
		s.CallerServiceInstanceID = "caller_service_instance_id"
	}
	if s.CalleeServiceName == "" {
		s.CalleeServiceName = "callee_service_name"
	}
	if s.CalleeServiceInstanceID == "" {
		s.CalleeServiceInstanceID = "callee_service_instance_id"
	}
	if s.Count == "" {
		s.Count = "count"
	}
	if s.ErrorCount == "" {
		s.ErrorCount = "error_count"
	}
}

const (
	defaultServiceIP       = "localhost"
	defaultServicePort     = 17271
	defaultServiceLogLevel = "info"

	defaultDorisDatabase   = "otel"
	defaultDorisTable      = "otel_traces"
	defaultDorisGraphTable = "otel_traces_graph"
)

func (c *Config) Init(configPath string) error {
	vip := viper.New()
	vip.SetConfigFile(configPath)

	err := vip.ReadInConfig()
	if err != nil {
		return err
	}

	err = vip.Unmarshal(c)
	if err != nil {
		return err
	}

	if c.Service == nil {
		c.Service = &ServiceConfig{}
	}

	if c.Doris == nil {
		c.Doris = &DorisConfig{}
	}

	if c.Doris.SchemaMapping == nil {
		c.Doris.SchemaMapping = &SchemaMapping{}
	}

	if c.Doris.GraphSchemaMapping == nil {
		c.Doris.GraphSchemaMapping = &GraphSchemaMapping{}
	}

	return nil
}

func (c *Config) Validate() error {
	var err error
	if c.Service.IP == "" {
		c.Service.IP = defaultServiceIP
	}

	if c.Service.Port == 0 {
		c.Service.Port = defaultServicePort
	}

	if c.Service.LogLevel == "" {
		c.Service.LogLevel = defaultServiceLogLevel
	}

	if c.Service.TimeoutSecond < 0 {
		err = errors.Join(err, errors.New("service.timeout must be greater than or equal to 0"))
	}

	if c.Doris.Endpoint == "" {
		err = errors.Join(err, errors.New("doris.endpoint must be specified"))
	}

	if c.Doris.Username == "" {
		err = errors.Join(err, errors.New("doris.username must be specified"))
	}

	if c.Doris.Database == "" {
		c.Doris.Database = defaultDorisDatabase
	}

	if c.Doris.Table == "" {
		c.Doris.Table = defaultDorisTable
	}
	c.Doris.SchemaMapping.FillDefaultValues()

	if c.Doris.GraphTable == "" {
		c.Doris.GraphTable = defaultDorisGraphTable
	}
	c.Doris.GraphSchemaMapping.FillDefaultValues()

	if c.Doris.TimeZone == "" {
		c.Doris.Location = time.Local
	} else {
		location, errT := time.LoadLocation(c.Doris.TimeZone)
		if errT != nil {
			err = errors.Join(err, errors.New("invalid timezone"))
		} else {
			c.Doris.Location = location
		}
	}

	// Preventing SQL Injection Attacks
	re := regexp.MustCompile(`^[a-zA-Z0-9_]+$`)
	if !re.MatchString(c.Doris.Database) {
		err = errors.Join(err, errors.New("doris.database must be alphanumeric and underscore"))
	}
	if !re.MatchString(c.Doris.Table) {
		err = errors.Join(err, errors.New("doris.table_name must be alphanumeric and underscore"))
	}

	return err
}

func (c *ServiceConfig) Address() string {
	return fmt.Sprintf("%s:%d", c.IP, c.Port)
}

func (c *DorisConfig) DSN() string {
	return fmt.Sprintf("%s:%s@tcp(%s)/%s", c.Username, c.Password, c.Endpoint, c.Database)
}

func (c *DorisConfig) TableFullName() string {
	return fmt.Sprintf("%s.%s", c.Database, c.Table)
}

func (c *DorisConfig) GraphTableFullName() string {
	return fmt.Sprintf("%s.%s", c.Database, c.GraphTable)
}

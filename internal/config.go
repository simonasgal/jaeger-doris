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
	Endpoint           string            `yaml:"endpoint" mapstructure:"endpoint"`
	Username           string            `yaml:"username" mapstructure:"username"`
	Password           string            `yaml:"password" mapstructure:"password"`
	Database           string            `yaml:"database" mapstructure:"database"`
	Table              string            `yaml:"table" mapstructure:"table"`
	FieldsMapping      map[string]string `yaml:"fields_mapping" mapstructure:"fields_mapping"`
	GraphTable         string            `yaml:"graph_table" mapstructure:"graph_table"`
	GraphFieldsMapping map[string]string `yaml:"graph_fields_mapping" mapstructure:"graph_fields_mapping"`
	TimeZone           string            `yaml:"timezone" mapstructure:"timezone"` // doris does not handle time zones and needs to be handled manually

	Location *time.Location `yaml:"-"`
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

	defaultMapping := map[string]string{
		SpanProcessAttributeServiceName:     SpanProcessAttributeServiceName,     // service_name
		SpanAttributeStartTime:              SpanAttributeStartTime,              // timestamp
		"service_instance_id":               "service_instance_id",               // service_instance_id (unused)
		SpanAttributeTraceID:                SpanAttributeTraceID,                // trace_id
		SpanAttributeSpanID:                 SpanAttributeSpanID,                 // span_id
		"trace_state":                       "trace_state",                       // trace_state (unused)
		SpanReferenceChildOfAttributeSpanID: SpanReferenceChildOfAttributeSpanID, // parent_span_id
		SpanAttributeOperationName:          SpanAttributeOperationName,          // span_name
		SpanTagAttributeSpanKind:            SpanTagAttributeSpanKind,            // span_kind
		"end_time":                          "end_time",                          // end_time (unused)
		SpanAttributeDuration:               SpanAttributeDuration,               // duration
		SpanAttributeTags:                   SpanAttributeTags,                   // span_attributes
		SpanAttributeLogs:                   SpanAttributeLogs,                   // events
		SpanAttributeReferencesFollowsFrom:  SpanAttributeReferencesFollowsFrom,  // links
		SpanTagAttributeStatusDescription:   SpanTagAttributeStatusDescription,   // status_message
		SpanTagAttributeStatusCode:          SpanTagAttributeStatusCode,          // status_code
		SpanProcessAttributeTags:            SpanProcessAttributeTags,            // resource_attributes
		"scope_name":                        "scope_name",                        // scope_name (unused)
		"scope_version":                     "scope_version",                     // scope_version (unused)
	}

	if c.Doris.FieldsMapping != nil {
		for k, v := range c.Doris.FieldsMapping {
			defaultMapping[k] = v
		}
	}
	c.Doris.FieldsMapping = defaultMapping

	if c.Doris.GraphTable == "" {
		c.Doris.GraphTable = defaultDorisGraphTable
	}

	defaultGraphMapping := map[string]string{
		GraphEdgeTimeStamp:           GraphEdgeTimeStamp,           // timestamp
		GraphEdgeParent:              GraphEdgeParent,              // caller_service_name
		"caller_service_instance_id": "caller_service_instance_id", // caller_service_instance_id (unused)
		GraphEdgeChild:               GraphEdgeChild,               // callee_service_name
		"callee_service_instance_id": "callee_service_instance_id", // callee_service_instance_id (unused)
		GraphEdgeCallCount:           GraphEdgeCallCount,           // count
		"error_count":                "error_count",                // error_count (unused)
	}
	if c.Doris.GraphFieldsMapping != nil {
		for k, v := range c.Doris.GraphFieldsMapping {
			defaultGraphMapping[k] = v
		}
	}
	c.Doris.GraphFieldsMapping = defaultGraphMapping

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

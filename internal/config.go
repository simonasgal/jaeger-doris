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
	Endpoint       string `yaml:"endpoint" mapstructure:"endpoint"`
	Username       string `yaml:"username" mapstructure:"username"`
	Password       string `yaml:"password" mapstructure:"password"`
	Database       string `yaml:"database" mapstructure:"database"`
	TableName      string `yaml:"table_name" mapstructure:"table_name"`
	GraphTableName string `yaml:"graph_table_name" mapstructure:"graph_table_name"`
	TimeZone       string `yaml:"timezone" mapstructure:"timezone"` // doris does not handle time zones and needs to be handled manually

	Location *time.Location `yaml:"-"`
}

const (
	defaultServiceIP            = "localhost"
	defaultServicePort          = 17271
	defaultServiceLogLevel      = "info"
	defaultServiceTimeoutSecond = 60

	defaultDorisDatabase       = "otel"
	defaultDorisTableName      = "otel_traces"
	defaultDorisGraphTableName = "otel_traces_graph"
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

	if c.Doris.TableName == "" {
		c.Doris.TableName = defaultDorisTableName
	}

	if c.Doris.GraphTableName == "" {
		c.Doris.GraphTableName = defaultDorisTableName
	}

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
	if !re.MatchString(c.Doris.TableName) {
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
	return fmt.Sprintf("%s.%s", c.Database, c.TableName)
}

func (c *DorisConfig) GraphTableFullName() string {
	return fmt.Sprintf("%s.%s", c.Database, c.GraphTableName)
}

package internal

import "fmt"

type Config struct {
	Service *ServiceConfig
	Doris   *DorisConfig
}

type ServiceConfig struct {
	IP            string
	Port          int32
	LogLevel      string
	TimeOutSecond int64
}

type DorisConfig struct {
	Endpoint  string
	Username  string
	Password  string
	Database  string
	TableName string
	TimeZone  string
}

func NewDefaultConfig() *Config {
	return &Config{
		Service: &ServiceConfig{
			IP:            "localhost",
			Port:          5000,
			LogLevel:      "debug",
			TimeOutSecond: 60,
		},
		Doris: &DorisConfig{
			Endpoint:  "localhost:9030",
			Username:  "admin",
			Password:  "admin",
			Database:  "otel2",
			TableName: "traces",
			TimeZone:  "Asia/Shanghai",
		},
	}
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

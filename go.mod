module github.com/joker-star-l/jaeger-doris

go 1.22.3

require (
	// cannot use higher version: https://github.com/go-sql-driver/mysql/issues/1602
	github.com/go-sql-driver/mysql v1.7.1
	github.com/jaegertracing/jaeger v1.59.0
	github.com/mattn/go-isatty v0.0.20
	go.uber.org/zap v1.27.0
	google.golang.org/grpc v1.65.0
)

require (
	github.com/opentracing/opentracing-go v1.2.0
	github.com/stretchr/testify v1.9.0
)

require (
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	go.opentelemetry.io/otel v1.28.0 // indirect
	go.opentelemetry.io/otel/trace v1.28.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/net v0.27.0 // indirect
	golang.org/x/sys v0.22.0 // indirect
	golang.org/x/text v0.16.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240701130421-f6361c86f094 // indirect
	google.golang.org/protobuf v1.34.2 // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

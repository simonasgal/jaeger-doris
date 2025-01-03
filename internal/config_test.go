package internal

import (
	"testing"

	"github.com/stretchr/testify/require"
)

const configPath = "../testdata/config.yaml"

func TestConfig_InitAndValidate(t *testing.T) {
	cfg := &Config{}

	err := cfg.Init(configPath)
	require.NoError(t, err)

	require.Equal(t, "127.0.0.1", cfg.Service.IP)
	require.Equal(t, "DEBUG", cfg.Service.LogLevel)

	require.Equal(t, "127.0.0.1:9030", cfg.Doris.Endpoint)
	require.Equal(t, "otel2", cfg.Doris.Database)

	require.Equal(t, "traces", cfg.Doris.TableName)
	require.Equal(t, "metrics", cfg.Doris.MetricTableName)

	require.NoError(t, cfg.Validate())
}

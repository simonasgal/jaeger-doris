package internal

import (
	"testing"

	"github.com/jaegertracing/jaeger/model"
	"github.com/stretchr/testify/require"
)

func TestTraceIDToString(t *testing.T) {
	traceID := model.TraceID{
		High: 0,
		Low:  0,
	}

	require.Equal(t, "00000000000000000000000000000000", traceIDToString(traceID))
}

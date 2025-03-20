package internal

import (
	"github.com/jaegertracing/jaeger/model"
)

const maxLen = 1024

func sanitizeTrace(trace *model.Trace) *model.Trace {
	if trace == nil || len(trace.Spans) == 0 {
		return trace
	}

	for _, s := range trace.Spans {
		for ti := range s.Tags {
			if s.Tags[ti].VType == model.ValueType_STRING && len(s.Tags[ti].VStr) > maxLen {
				s.Tags[ti].VStr = s.Tags[ti].VStr[:maxLen-3] + "..."
			}
		}
	}
	return trace
}

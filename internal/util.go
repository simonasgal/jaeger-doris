package internal

import (
	"github.com/jaegertracing/jaeger/model"
)

const maxLen = 1024

func sanitizeTrace(trace *model.Trace) *model.Trace {
	if trace == nil || len(trace.Spans) == 0 {
		return trace
	}

	for i := range trace.Spans {
		s := trace.Spans[i]
		for ti := range s.Tags {
			t := &s.Tags[ti]
			if t.VType == model.ValueType_STRING {
				if len(t.VStr) > maxLen {
					t.VStr = t.VStr[:maxLen-3] + "..."
				}
			}
		}
	}
	return trace
}

func ellipsis(s string, maxLen int) string {
	runes := []rune(s)
	if len(runes) <= maxLen {
		return s
	}
	if maxLen < 3 {
		maxLen = 3
	}
	return string(runes[0:maxLen-3]) + "..."
}

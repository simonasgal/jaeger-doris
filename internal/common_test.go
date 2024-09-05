package internal

import (
	"fmt"
	"testing"

	"github.com/jaegertracing/jaeger/model"
)

func TestTraceIDToString(t *testing.T) {
	traceID := model.TraceID{
		High: 0,
		Low:  0,
	}

	fmt.Println(traceIDToString(traceID))
}

func TestMap(t *testing.T) {
	m := make(map[string]any)
	a := m["key"]
	fmt.Println(a == nil)
	s, err := a.(string)
	if !err {
		fmt.Println("error")
	}
	fmt.Println(s)
}

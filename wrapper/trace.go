package wrapper

import "github.com/DistributedClocks/tracing"


func CreateTrace(tracer *tracing.Tracer) *tracing.Trace {
	if tracer == nil {
		return &tracing.Trace{Tracer: nil}
	}
	return tracer.CreateTrace()
}

func ReceiveToken(tracer *tracing.Tracer, retToken tracing.TracingToken) *tracing.Trace {
	if tracer == nil {
		return nil
	}
	return tracer.ReceiveToken(retToken)
}

func GenerateToken(trace *tracing.Trace) tracing.TracingToken {
	if trace.Tracer == nil {
		return nil
	}
	return trace.GenerateToken()
}

func RecordAction(trace *tracing.Trace, lib interface{}) {
	if trace.Tracer != nil {
		trace.RecordAction(lib)
	}
}

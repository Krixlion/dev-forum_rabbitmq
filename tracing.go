package rabbitmq

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type amqpHeadersCarrier map[string]interface{}

func (a amqpHeadersCarrier) Get(key string) string {
	v, ok := a[key]
	if !ok {
		return ""
	}
	return v.(string)
}

func (a amqpHeadersCarrier) Set(key string, value string) {
	a[key] = value
}

func (a amqpHeadersCarrier) Keys() []string {
	i := 0
	r := make([]string, len(a))

	for k := range a {
		r[i] = k
		i++
	}

	return r
}

// InjectAMQPHeaders injects the trace data from the context into the header map.
func InjectAMQPHeaders(ctx context.Context) map[string]interface{} {
	h := make(amqpHeadersCarrier)
	otel.GetTextMapPropagator().Inject(ctx, h)
	return h
}

// ExtractAMQPHeaders extracts the trace data from the header and puts it into the context.
func ExtractAMQPHeaders(ctx context.Context, headers map[string]interface{}) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, amqpHeadersCarrier(headers))
}

func setSpanErr(span trace.Span, err error) {
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}

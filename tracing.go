package rabbitmq

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// injectAMQPHeaders injects the trace data from the context into the header map.
func injectAMQPHeaders(ctx context.Context) map[string]interface{} {
	m := map[string]string{}
	otel.GetTextMapPropagator().Inject(ctx, propagation.MapCarrier(m))

	headers := make(map[string]interface{}, len(m))
	for k, v := range m {
		headers[k] = v
	}

	return headers
}

// extractAMQPHeaders extracts the trace data from the header and puts it
// into the returned context. Any extracted non-string values are discarded.
func extractAMQPHeaders(ctx context.Context, headers map[string]interface{}) context.Context {
	m := make(map[string]string, len(headers))
	for k, v := range headers {
		str, ok := v.(string)
		if !ok {
			continue
		}
		m[k] = str
	}

	return otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(m))
}

// ExtractAMQPHeaders extracts the trace data from the header and puts it into the returned context.
func ExtractMessageHeaders(ctx context.Context, headers map[string]string) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(headers))
}

func setSpanErr(span trace.Span, err error) {
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}

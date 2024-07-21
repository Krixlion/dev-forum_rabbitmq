package rabbitmq

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/embedded"
)

var _ Logger = (*nullLogger)(nil)

type nullLogger struct{}

func (nullLogger) Log(context.Context, string, ...any) {}

var _ trace.Tracer = (*nullTracer)(nil)

type nullTracer struct {
	embedded.Tracer
}

func (nullTracer) Start(ctx context.Context, spanName string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return ctx, nullSpan{}
}

type nullSpan struct {
	embedded.Span
}

func (nullSpan) AddLink(link trace.Link)                             {}
func (nullSpan) End(options ...trace.SpanEndOption)                  {}
func (nullSpan) AddEvent(name string, options ...trace.EventOption)  {}
func (nullSpan) IsRecording() bool                                   { return false }
func (nullSpan) RecordError(err error, options ...trace.EventOption) {}
func (nullSpan) SpanContext() trace.SpanContext                      { return trace.SpanContext{} }
func (nullSpan) SetStatus(code codes.Code, description string)       {}
func (nullSpan) SetName(name string)                                 {}
func (nullSpan) SetAttributes(kv ...attribute.KeyValue)              {}
func (nullSpan) TracerProvider() trace.TracerProvider                { return nil }

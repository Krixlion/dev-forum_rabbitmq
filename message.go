package rabbitmq

import (
	"time"
)

type ContentType string

const (
	ContentTypeJson ContentType = "application/json"
	ContentTypeText ContentType = "text/plain"
)

type Message struct {
	Route

	Body        []byte
	ContentType ContentType
	Timestamp   time.Time
	Headers     map[string]string
}

type Route struct {
	ExchangeName string
	ExchangeType string
	RoutingKey   string
}

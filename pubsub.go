package rabbitmq

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel/trace"
)

func (mq *RabbitMQ) Publish(ctx context.Context, msg Message) error {
	ctx, span := mq.opts.tracer.Start(ctx, "rabbitmq.Publish", trace.WithSpanKind(trace.SpanKindProducer))
	defer span.End()

	if err := mq.prepareExchange(ctx, msg.Route); err != nil {
		setSpanErr(span, err)
		return err
	}

	if err := mq.publish(ctx, msg); err != nil {
		setSpanErr(span, err)
		return err
	}

	return nil
}

// prepareExchange validates a message and declares a RabbitMQ exchange derived from the message.
func (mq *RabbitMQ) prepareExchange(ctx context.Context, route Route) error {
	ctx, span := mq.opts.tracer.Start(ctx, "rabbitmq.prepareExchange")
	defer span.End()

	ch := mq.askForChannel()
	defer ch.Close()

	if err := ctx.Err(); err != nil {
		setSpanErr(span, err)
		return err
	}

	done, err := mq.breaker.Allow()
	if err != nil {
		setSpanErr(span, err)
		return err
	}

	err = ch.ExchangeDeclare(
		route.ExchangeName, // name
		route.ExchangeType, // type
		true,               // durable
		false,              // auto-deleted
		false,              // internal
		false,              // no-wait
		nil,                // arguments
	)
	if err != nil {
		done(!isConnectionError(err))
		setSpanErr(span, err)
		return err
	}
	done(true)

	return nil
}

func (mq *RabbitMQ) publish(ctx context.Context, msg Message) error {
	ctx, span := mq.opts.tracer.Start(ctx, "rabbitmq.publish", trace.WithSpanKind(trace.SpanKindProducer))
	defer span.End()

	ch := mq.askForChannel()
	defer ch.Close()

	if err := ctx.Err(); err != nil {
		setSpanErr(span, err)
		return err
	}

	done, err := mq.breaker.Allow()
	if err != nil {
		setSpanErr(span, err)
		return err
	}

	err = ch.PublishWithContext(ctx,
		msg.ExchangeName, // exchange
		msg.RoutingKey,   // routing key
		false,            // mandatory
		false,            // immediate
		amqp.Publishing{
			ContentType: string(msg.ContentType),
			Body:        msg.Body,
			Timestamp:   msg.Timestamp,
			Headers:     injectAMQPHeaders(ctx),
		},
	)
	if err != nil {
		done(!isConnectionError(err))
		setSpanErr(span, err)
		return err
	}
	done(true)
	return nil
}

func (mq *RabbitMQ) Consume(ctx context.Context, command string, route Route) (<-chan Message, error) {
	ctx, span := mq.opts.tracer.Start(ctx, "rabbitmq.Consume init", trace.WithSpanKind(trace.SpanKindConsumer))
	defer span.End()

	messages := make(chan Message)
	ch := mq.askForChannel()

	queue, err := mq.prepareQueue(ctx, command, route)
	if err != nil {
		setSpanErr(span, err)
		return nil, err
	}

	done, err := mq.breaker.Allow()
	if err != nil {
		setSpanErr(span, err)
		return nil, err
	}

	deliveries, err := ch.ConsumeWithContext(ctx,
		queue.Name,      // queue
		mq.consumerName, // consumer
		false,           // auto ack
		false,           // exclusive
		false,           // no local
		false,           // no wait
		nil,             // args
	)
	if err != nil {
		done(!isConnectionError(err))
		setSpanErr(span, err)
		return nil, err
	}
	done(true)

	go func() {
		for {
			select {
			case delivery := <-deliveries:
				func() {
					ctx, span := mq.opts.tracer.Start(extractAMQPHeaders(ctx, delivery.Headers), "rabbitmq.Consume", trace.WithSpanKind(trace.SpanKindConsumer))
					defer span.End()

					if err := delivery.Ack(false); err != nil {
						setSpanErr(span, err)
						mq.opts.logger.Log(ctx, "Failed to acknowledge message delivery", "err", err)
						return
					}

					message := Message{
						Route:       route,
						Body:        delivery.Body,
						ContentType: ContentType(delivery.ContentType),
						Timestamp:   delivery.Timestamp,
					}

					// Non-blocking send to unbuffered channel
					select {
					case messages <- message:
					default:
						return
					}
				}()
			case <-ctx.Done():
				close(messages)
				return
			}
		}
	}()

	return messages, nil
}

func (mq *RabbitMQ) prepareQueue(ctx context.Context, command string, route Route) (queue amqp.Queue, err error) {
	ctx, span := mq.opts.tracer.Start(ctx, "rabbitmq.prepareQueue")
	defer span.End()

	ch := mq.askForChannel()

	done, err := mq.breaker.Allow()
	if err != nil {
		setSpanErr(span, err)
		return amqp.Queue{}, err
	}

	queue, err = ch.QueueDeclare(
		command, // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	if err != nil {
		setSpanErr(span, err)
		done(!isConnectionError(err))
		return amqp.Queue{}, err
	}
	done(true)

	if err := ctx.Err(); err != nil {
		setSpanErr(span, err)
		return amqp.Queue{}, err
	}

	if err := mq.prepareExchange(ctx, route); err != nil {
		setSpanErr(span, err)
		return amqp.Queue{}, err
	}

	done, err = mq.breaker.Allow()
	if err != nil {
		setSpanErr(span, err)
		return amqp.Queue{}, err
	}

	err = ch.QueueBind(
		queue.Name,         // queue name
		route.RoutingKey,   // routing key
		route.ExchangeName, // exchange
		false,              // Immediate
		nil,                // Additional args
	)
	if err != nil {
		setSpanErr(span, err)
		done(!isConnectionError(err))
		return amqp.Queue{}, err
	}
	done(true)

	return queue, nil
}

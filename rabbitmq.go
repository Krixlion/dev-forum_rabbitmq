package rabbitmq

import (
	"context"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sony/gobreaker"
)

const (
	connectionError = 1
	channelError    = 2
)

type RabbitMQ struct {
	consumerName string
	ctx          context.Context
	shutdown     context.CancelFunc
	config       Config
	url          string // Connection string to RabbitMQ broker.

	conn      *amqp.Connection
	breaker   *gobreaker.TwoStepCircuitBreaker
	connMutex sync.Mutex // Mutex protecting connection during reconnecting.

	notifyConnClose chan *amqp.Error        // Channel to watch for errors from the broker in order to renew the connection.
	publishQueue    chan Message            // Queue for messages waiting to be republished.
	readChannel     chan chan *amqp.Channel // Access channel for accessing the RabbitMQ Channel in a thread-safe way.

	opts options
}

// NewRabbitMQ returns a new initialized connection struct.
// It will manage the active connection in the background.
// Connection should be closed in order to shut it down gracefully.
//
//	func example() {
//		user := "guest"
//		pass := "guest"
//		host := "localhost"
//		port := "5672"
//		consumer := "user-service" //  Unique name for each consumer used to sign messages.
//
//		var customLogger Logger
//
//		// You can specify your own config or use rabbitmq.DefaultConfig() instead.
//		config := Config{
//			QueueSize:         100,
//			MaxWorkers:        50,
//			ReconnectInterval: time.Second * 2,
//			MaxRequests:       5,
//			ClearInterval:     time.Second * 5,
//			ClosedTimeout:     time.Second * 5,
//		}
//
//		// Logger and tracer are optional.
//		rabbit := rabbitmq.NewRabbitMQ(consumer, user, pass, host, port, config, WithLogger(customLogger))
//		defer rabbit.Close()
//	}
func NewRabbitMQ(consumer, user, pass, host, port string, config Config, opts ...Option) *RabbitMQ {
	url := fmt.Sprintf("amqp://%s:%s@%s:%s/", user, pass, host, port)
	ctx, cancel := context.WithCancel(context.Background())

	// Make sure there is at least one worker.
	if config.MaxWorkers == 0 {
		config.MaxWorkers++
	}

	mq := &RabbitMQ{
		consumerName: consumer,
		ctx:          ctx,
		shutdown:     cancel,

		publishQueue:    make(chan Message, config.QueueSize),
		readChannel:     make(chan chan *amqp.Channel),
		notifyConnClose: make(chan *amqp.Error, 16),

		connMutex: sync.Mutex{},
		url:       url,
		config:    config,
		opts:      defaultOptions(),
		breaker: gobreaker.NewTwoStepCircuitBreaker(gobreaker.Settings{
			Name:        consumer,
			MaxRequests: config.MaxRequests,
			Interval:    config.ClearInterval,
			Timeout:     config.ClosedTimeout,
		}),
	}

	for _, opt := range opts {
		opt.apply(&mq.opts)
	}

	defer mq.run()
	return mq
}

// run initializes the RabbitMQ connection and manages it in
// seperate goroutines while blocking the goroutine it was called from.
// You should use Close() in order to shutdown the connection.
func (mq *RabbitMQ) run() {
	mq.opts.logger.Log(mq.ctx, "Connecting to RabbitMQ")
	mq.ReDial(mq.ctx)

	go mq.runPublishQueue(mq.ctx)
	go mq.handleConnectionErrors(mq.ctx)
	go mq.handleChannelReads(mq.ctx)
}

// Close closes active connection gracefully.
func (mq *RabbitMQ) Close() error {
	mq.shutdown()

	if mq.conn != nil && !mq.conn.IsClosed() {
		mq.opts.logger.Log(mq.ctx, "Closing active connections")

		if err := mq.conn.Close(); err != nil {
			mq.opts.logger.Log(mq.ctx, "Failed to close active connections", "err", err)
			return err
		}
	}

	return nil
}

// runPublishQueue is meant to be run in a seperate goroutine.
func (mq *RabbitMQ) runPublishQueue(ctx context.Context) {
	preparedMessages := mq.prepareExchangePipelined(ctx, mq.publishQueue)
	mq.publishPipelined(ctx, preparedMessages)
}

// handleChannelReads is meant to be run in a seperate goroutine.
func (mq *RabbitMQ) handleChannelReads(ctx context.Context) {
	limiter := make(chan struct{}, mq.config.MaxWorkers)
	for {
		select {
		case req := <-mq.readChannel:
			limiter <- struct{}{}
			go func() {
				ctx, span := mq.opts.tracer.Start(ctx, "rabbitmq.handleChannelRead")
				defer span.End()
				defer func() { <-limiter }()
				succedeed, err := mq.breaker.Allow()
				if err != nil {
					req <- nil
					setSpanErr(span, err)
					return
				}

				channel, err := mq.conn.Channel()
				if err != nil {
					req <- nil
					mq.opts.logger.Log(ctx, "Failed to open a new channel", "err", err)
					succedeed(false)
					setSpanErr(span, err)
					return
				}

				succedeed(true)
				req <- channel
			}()
		case <-ctx.Done():
			return
		}
	}
}

// handleConnectionErrors is meant to be run in a seperate goroutine.
func (mq *RabbitMQ) handleConnectionErrors(ctx context.Context) {
	for {
		select {
		case e := <-mq.notifyConnClose:
			if e == nil {
				continue
			}
			mq.ReDial(ctx)

		case <-ctx.Done():
			return
		}
	}
}

// ReDial will keep reconecting until it succeeds.
func (mq *RabbitMQ) ReDial(ctx context.Context) {
	ctx, span := mq.opts.tracer.Start(ctx, "rabbitmq.ReDial")
	defer span.End()

	for {
		if err := ctx.Err(); err != nil {
			return
		}

		err := mq.dial()
		if err == nil {
			return
		}

		setSpanErr(span, err)

		mq.opts.logger.Log(ctx, "Failed to connect to RabbitMQ", "err", err)
		time.Sleep(mq.config.ReconnectInterval)
		mq.opts.logger.Log(ctx, "Reconnecting to RabbitMQ")
	}
}

// dial renews current TCP connection.
func (mq *RabbitMQ) dial() (err error) {
	_, span := mq.opts.tracer.Start(context.Background(), "rabbitmq.dial")
	defer func() {
		if err != nil {
			setSpanErr(span, err)
		}
		span.End()
	}()

	ok, err := mq.breaker.Allow()
	if err != nil {
		return err
	}

	conn, err := amqp.Dial(mq.url)
	if err != nil {
		// If not conn error then broker is available.
		ok(!isConnectionError(err))
		return err
	}
	ok(true)

	mq.connMutex.Lock()
	mq.notifyConnClose = conn.NotifyClose(mq.notifyConnClose)
	mq.conn = conn
	mq.connMutex.Unlock()

	return nil
}

// askForChannel returns a *amqp.Channel in a thread-safe way.
func (mq *RabbitMQ) askForChannel() *amqp.Channel {
	for {
		ask := make(chan *amqp.Channel)
		mq.readChannel <- ask

		channel := <-ask
		if channel != nil {
			return channel
		}

		time.Sleep(mq.config.ReconnectInterval)
	}
}

func errorType(code int) int {
	switch code {
	case
		amqp.ContentTooLarge,    // 311
		amqp.NoConsumers,        // 313
		amqp.AccessRefused,      // 403
		amqp.NotFound,           // 404
		amqp.ResourceLocked,     // 405
		amqp.PreconditionFailed: // 406
		return channelError

	case
		amqp.ConnectionForced, // 320
		amqp.InvalidPath,      // 402
		amqp.FrameError,       // 501
		amqp.SyntaxError,      // 502
		amqp.CommandInvalid,   // 503
		amqp.ChannelError,     // 504
		amqp.UnexpectedFrame,  // 505
		amqp.ResourceError,    // 506
		amqp.NotAllowed,       // 530
		amqp.NotImplemented,   // 540
		amqp.InternalError:    // 541
		fallthrough

	default:
		return connectionError
	}
}

func isConnectionError(err error) bool {
	if err, ok := err.(*amqp.Error); ok {
		return errorType(err.Code) == connectionError
	}
	return false
}

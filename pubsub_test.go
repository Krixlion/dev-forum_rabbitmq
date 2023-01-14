package rabbitmq_test

import (
	"context"
	"encoding/json"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	rabbitmq "github.com/krixlion/dev-forum_rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"

	"github.com/google/go-cmp/cmp"
)

const consumer = "TESTING"

var (
	port string
	host string
	user string
	pass string
)

func init() {
	var ok bool

	port, ok = os.LookupEnv("MQ_PORT")
	if !ok || port == "" {
		port = "5672"
	}

	host, ok = os.LookupEnv("MQ_HOST")
	if !ok || host == "" {
		host = "rabbitmq-service"
	}

	user, ok = os.LookupEnv("MQ_USER")
	if !ok || user == "" {
		user = "guest"
	}

	pass, ok = os.LookupEnv("MQ_PASS")
	if !ok || pass == "" {
		pass = "guest"
	}
}

func setUpMQ() *rabbitmq.RabbitMQ {
	config := rabbitmq.Config{
		QueueSize:         100,
		ReconnectInterval: time.Millisecond * 100,
		MaxRequests:       30,
		ClearInterval:     time.Second * 5,
		ClosedTimeout:     time.Second * 15,
		MaxWorkers:        10,
	}
	tracer := otel.Tracer("rabbitmq-test")
	mq := rabbitmq.NewRabbitMQ(consumer, user, pass, host, port, config, nil, tracer)
	return mq
}

func randomString(length int) string {
	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	v := make([]rune, length)
	for i := range v {
		v[i] = letters[rand.Intn(len(letters))]
	}
	return string(v)
}

func TestPubSub(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Pub/Sub integration test")
	}

	mq := setUpMQ()
	defer mq.Close()

	testData := randomString(5)
	data, err := json.Marshal(testData)
	if err != nil {
		t.Fatalf("Failed to marshal article, input: %+v, err: %s", testData, err)
	}

	testCases := []struct {
		desc    string
		msg     rabbitmq.Message
		wantErr bool
	}{
		{
			desc: "Test if a simple message is correctly published and consumed.",
			msg: rabbitmq.Message{
				Body:        data,
				ContentType: rabbitmq.ContentTypeJson,
				Timestamp:   time.Now().Round(time.Second),
				Route: rabbitmq.Route{
					ExchangeName: "test",
					ExchangeType: amqp.ExchangeTopic,
					RoutingKey:   "test.event." + strings.ToLower(randomString(5)),
				},
			},
			wantErr: false,
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			err := mq.Publish(ctx, tC.msg)
			if (err != nil) != tC.wantErr {
				t.Errorf("RabbitMQ.Publish() error = %+v\n, wantErr = %+v\n", err, tC.wantErr)
			}

			msgs, err := mq.Consume(ctx, "deleteArticle", tC.msg.Route)
			if (err != nil) != tC.wantErr {
				t.Errorf("RabbitMQ.Consume() error = %+v\n, wantErr = %+v\n", err, tC.wantErr)
			}

			msg := <-msgs
			if !cmp.Equal(tC.msg, msg) {
				t.Errorf("Messages are not equal:\n want = %+v\n got = %+v\n", tC.msg, msg)
			}
		})
	}
}

func TestPubSubPipeline(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Pub/Sub Pipeline integration test")
	}

	mq := setUpMQ()
	defer mq.Close()

	testData := randomString(5)
	data, err := json.Marshal(testData)
	if err != nil {
		t.Fatalf("Failed to marshal article:\n input = %+v\n, err = %s", testData, err)
	}

	testCases := []struct {
		desc    string
		msg     rabbitmq.Message
		wantErr bool
	}{
		{
			desc: "Test if a simple message is correctly published through a pipeline and consumed.",
			msg: rabbitmq.Message{
				Body:        data,
				ContentType: rabbitmq.ContentTypeJson,
				Timestamp:   time.Now().Round(time.Second),
				Route: rabbitmq.Route{
					ExchangeName: "test",
					ExchangeType: amqp.ExchangeTopic,
					RoutingKey:   "test.event." + strings.ToLower(randomString(5)),
				},
			},
			wantErr: false,
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			err := mq.Enqueue(tC.msg)
			if (err != nil) != tC.wantErr {
				t.Errorf("RabbitMQ.Enqueue() error = %+v\n wantErr = %+v\n", err, tC.wantErr)
			}

			messages, err := mq.Consume(ctx, "createArticle", tC.msg.Route)
			if (err != nil) != tC.wantErr {
				t.Errorf("RabbitMQ.Consume() error = %+v\n wantErr = %+v\n", err, tC.wantErr)
			}

			msg := <-messages
			if !cmp.Equal(tC.msg, msg) {
				t.Errorf("Messages are not equal:\n want = %+v\n  got = %+v\n", tC.msg, msg)
			}
		})
	}
}

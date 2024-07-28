package rabbitmq_test

import (
	"context"
	"encoding/json"
	"math/rand/v2"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/joho/godotenv"
	rabbitmq "github.com/krixlion/dev_forum-rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/goleak"
)

var (
	port string
	host string
	user string
	pass string
)

func setUpMQ(t *testing.T) *rabbitmq.RabbitMQ {
	const consumer = "TESTING"

	if err := godotenv.Load(); err != nil {
		t.Logf("Failed to load env file, using default settings, err: %s", err)
	}

	var ok bool
	port, ok = os.LookupEnv("MQ_PORT")
	if !ok || port == "" {
		port = "5672"
	}

	host, ok = os.LookupEnv("MQ_HOST")
	if !ok || host == "" {
		host = "localhost"
	}

	user, ok = os.LookupEnv("MQ_USER")
	if !ok || user == "" {
		user = "guest"
	}

	pass, ok = os.LookupEnv("MQ_PASS")
	if !ok || pass == "" {
		pass = "guest"
	}

	config := rabbitmq.Config{
		QueueSize:         100,
		ReconnectInterval: time.Millisecond * 100,
		MaxRequests:       30,
		ClearInterval:     time.Second * 5,
		ClosedTimeout:     time.Second * 15,
		MaxWorkers:        10,
	}
	return rabbitmq.NewRabbitMQ(consumer, user, pass, host, port, config)
}

func randomString(length int) string {
	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	v := make([]rune, length)
	for i := range v {
		v[i] = letters[rand.IntN(len(letters))]
	}
	return string(v)
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestPubSub(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Pub/Sub integration test...")
	}

	testData := randomString(5)
	data, err := json.Marshal(testData)
	if err != nil {
		t.Fatalf("Failed to marshal string, input: %+v, err: %s", testData, err)
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

			mq := setUpMQ(t)
			defer mq.Close()

			err := mq.Publish(ctx, tC.msg)
			if (err != nil) != tC.wantErr {
				t.Errorf("RabbitMQ.Publish() error = %+v\n, wantErr = %+v\n", err, tC.wantErr)
				return
			}

			msgs, err := mq.Consume(ctx, "deleteArticle", tC.msg.Route)
			if (err != nil) != tC.wantErr {
				t.Errorf("RabbitMQ.Consume() error = %+v\n, wantErr = %+v\n", err, tC.wantErr)
				return
			}

			msg := <-msgs
			if !cmp.Equal(tC.msg, msg) {
				t.Errorf("Messages are not equal:\n want = %+v\n got = %+v\n diff = %+v\n", tC.msg, msg, cmp.Diff(tC.msg, msg))
				return
			}
		})
	}
}

func TestIfExchangeIsCreatedBeforeBindingQueue(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test...")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	mq := setUpMQ(t)
	defer mq.Close()

	_, err := mq.Consume(ctx, "testingQueue", rabbitmq.Route{
		ExchangeName: randomString(7),
		ExchangeType: "topic",
		RoutingKey:   randomString(6),
	})
	if err != nil {
		t.Fatalf("RabbitMQ.Consume() error = %+v\n", err)
	}
}

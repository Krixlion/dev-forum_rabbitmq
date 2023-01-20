package rabbitmq_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	rabbitmq "github.com/krixlion/dev_forum-rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestPubSubPipeline(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Pub/Sub Pipeline integration test")
	}
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

			mq := setUpMQ()
			defer mq.Close()

			err := mq.Enqueue(tC.msg)
			if (err != nil) != tC.wantErr {
				t.Errorf("RabbitMQ.Enqueue() error = %+v\n wantErr = %+v\n", err, tC.wantErr)
				return
			}

			messages, err := mq.Consume(ctx, "createArticle", tC.msg.Route)
			if (err != nil) != tC.wantErr {
				t.Errorf("RabbitMQ.Consume() error = %+v\n wantErr = %+v\n", err, tC.wantErr)
				return
			}

			msg := <-messages
			if !cmp.Equal(tC.msg, msg) {
				t.Errorf("Messages are not equal:\n want = %+v\n  got = %+v\n diff = %+v\n", tC.msg, msg, cmp.Diff(tC.msg, msg))
				return
			}
		})
	}
}

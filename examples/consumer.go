package examples

import (
	"context"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"log"
	"time"
)

func InitPushConsumer(topic string, f func(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error), selector consumer.MessageSelector, opts ...consumer.Option) {
	c, err := rocketmq.NewPushConsumer(opts...)
	if err != nil {
		log.Fatalf("Failed to new push consumer, error: %v\n", err)
	}
	err = c.Subscribe(topic, selector, f)
	if err != nil {
		log.Fatalf("Failed to subscribe topic, error: %v\n", err)
	}
	if err := c.Start(); err != nil {
		log.Fatalf("Failed to start consumer, error %v\n", err)
	}
	time.Sleep(time.Hour)
	if err := c.Shutdown(); err != nil {
		log.Fatalf("Failed to shutdown consumer, error: %v\n", err)
	}
}

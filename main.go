package main

import (
	"context"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"log"
	"math/rand"
	"rocketmq-learn/examples"
	"strconv"
	"time"
)

const (
	nameserverAddr    = "127.0.0.1:9876"
	brokerAddr        = "127.0.0.1:10911"
	producerGroupName = "producer"
	consumerGroupName = "consumer"

	topic = "propertyTopic"
)

func product() {
	examples.InitProducer(func(p *rocketmq.Producer) {
		rand.Seed(time.Now().UnixNano())
		age := rand.Int()
		if age%2 == 0 {
			log.Println("ok")
		}
		properties := map[string]string{
			"name": "PPG007",
			"age":  strconv.Itoa(age),
		}
		examples.SendMessageWithCustomProperty(context.Background(), p, topic, properties)
	},
		producer.WithNsResolver(primitive.NewPassthroughResolver([]string{nameserverAddr})),
		producer.WithGroupName(producerGroupName),
	)
}

func consume() {
	examples.InitPushConsumer(topic, func(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		for _, msg := range msgs {
			log.Println(string(msg.Message.Body))
		}
		return consumer.ConsumeSuccess, nil
	},
		consumer.MessageSelector{
			Type:       consumer.SQL92,
			Expression: "name = PPG007",
		},
		consumer.WithGroupName(consumerGroupName),
		consumer.WithNsResolver(primitive.NewPassthroughResolver([]string{nameserverAddr})),
		consumer.WithConsumeFromWhere(consumer.ConsumeFromTimestamp),
	)
}

func main() {
	examples.CreateTopicIfNotExist(context.Background(), []string{nameserverAddr}, brokerAddr, topic)
	product()
	time.Sleep(time.Second * 3)
	consume()
}

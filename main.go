package main

import (
	"context"
	"encoding/json"
	"github.com/PPG007/rocketmq-client-go/v2"
	"github.com/PPG007/rocketmq-client-go/v2/primitive"
	"github.com/PPG007/rocketmq-client-go/v2/producer"
	"rocketmq-learn/examples"
)

const (
	nameserverAddr    = "127.0.0.1:9876"
	brokerAddr        = "127.0.0.1:10911"
	producerGroupName = "producer1"
	consumerGroupName = "consumer"
)

func main() {
	selector := producer.NewIdQueueSelector()
	examples.InitProducer(func(p *rocketmq.Producer) {
		examples.SendInOrder(context.Background(), p, "InOrderTopic")
	},
		producer.WithNsResolver(primitive.NewPassthroughResolver([]string{nameserverAddr})),
		producer.WithQueueSelector(selector),
		producer.WithGroupName(producerGroupName),
	)
	//fmt.Println("=======================================================================================================================")
	//time.Sleep(3 * time.Second)
	//examples.InitPushConsumer("InOrderTopic", func(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
	//	for _, msg := range msgs {
	//		log.Println(string(msg.Message.Body))
	//	}
	//	return consumer.ConsumeSuccess, nil
	//},
	//	consumer.WithGroupName(consumerGroupName),
	//	consumer.WithNsResolver(primitive.NewPassthroughResolver([]string{nameserverAddr})),
	//	consumer.WithConsumerOrder(true),
	//	consumer.WithConsumeFromWhere(consumer.ConsumeFromFirstOffset),
	//)
}

func String(a any) string {
	b, err := json.Marshal(a)
	if err != nil {
		panic(err)
	}
	return string(b)
}

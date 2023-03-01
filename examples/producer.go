package examples

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"log"
	"strconv"
	"sync"
)

func InitProducer(f func(*rocketmq.Producer), opts ...producer.Option) {
	p, err := rocketmq.NewProducer(opts...)
	if err != nil {
		log.Fatalln("Failed to new producer:", err)
	}
	if err := p.Start(); err != nil {
		log.Fatalf("Failed to start producer, error: %v\n", err)
	}
	f(&p)
	if err := p.Shutdown(); err != nil {
		log.Fatalf("Failed to shutdown producer, error: %v\n", err)
	}
}

func SendSync(ctx context.Context, p *rocketmq.Producer, topic string) {
	sendResult, err := (*p).SendSync(ctx, primitive.NewMessage(topic, []byte("Hello")))
	if err != nil {
		log.Println("Failed to send message:", err)
		return
	}
	fmt.Println(sendResult)
}

func SendAsync(ctx context.Context, p *rocketmq.Producer, topic string) {
	wg := sync.WaitGroup{}
	wg.Add(1)
	err := (*p).SendAsync(ctx, func(ctx context.Context, result *primitive.SendResult, err error) {
		if err != nil {
			log.Printf("Get respose from broker error: %v\n", err)
		} else {
			log.Printf("Get response from broker success, result: %v\n", result.String())
		}
		wg.Done()
	}, primitive.NewMessage(topic, []byte("SendAsync")))
	if err != nil {
		log.Println("Failed to send message:", err)
	}
	wg.Wait()
}

func SendOneWay(ctx context.Context, p *rocketmq.Producer, topic string) {
	err := (*p).SendOneWay(ctx, primitive.NewMessage(topic, []byte("send one way")))
	if err != nil {
		log.Printf("Failed to send one way: %v\n", err)
	}
}

func SendInOrder(ctx context.Context, p *rocketmq.Producer, topic string) {
	orderSteps := GenOrderSteps()
	for _, orderStep := range orderSteps {
		msg := &primitive.Message{
			Topic: topic,
			Body:  []byte(orderStep.String()),
		}
		msg.WithShardingKey(strconv.FormatInt(orderStep.Id, 10))
		_, err := (*p).SendSync(ctx, msg)
		if err != nil {
			log.Printf("Failed to send messages in order, error: %v\n", err)
		}
	}
}

func BatchSend(ctx context.Context, p *rocketmq.Producer, topic string) {
	total := 100
	messages := make([]*primitive.Message, 0, total)
	for i := 0; i < total; i++ {
		messages = append(messages, primitive.NewMessage(topic, []byte(fmt.Sprintf("batch message %d", i))))
	}
	_, err := (*p).SendSync(ctx, messages...)
	if err != nil {
		log.Printf("Failed to batch send messages, error: %v\n", err)
	}
}

func SendDelayMessage(ctx context.Context, p *rocketmq.Producer, topic string) {
	msg := primitive.NewMessage(topic, []byte("delay message"))
	msg.WithDelayTimeLevel(2)
	_, err := (*p).SendSync(ctx, msg)
	if err != nil {
		log.Printf("Failed to send delay message, error: %v\n", err)
	}
}

func SendMessageWithCustomProperty(ctx context.Context, p *rocketmq.Producer, topic string, properties map[string]string) {
	msg := primitive.NewMessage(topic, []byte("with property message"))
	msg.WithProperties(properties)
	_, err := (*p).SendSync(ctx, msg)
	if err != nil {
		log.Printf("Failed to send message with custom property, error: %v\n", err)
	}
}

type OrderStep struct {
	Id   int64
	Desc string
}

func GenOrderSteps() []OrderStep {
	result := []OrderStep{
		{
			Id:   15103111039,
			Desc: "创建",
		},
		{
			Id:   15103111065,
			Desc: "创建",
		},
		{
			Id:   15103111039,
			Desc: "付款",
		},
		{
			Id:   15103117235,
			Desc: "创建",
		},
		{
			Id:   15103111065,
			Desc: "付款",
		},
		{
			Id:   15103117235,
			Desc: "付款",
		},
		{
			Id:   15103111065,
			Desc: "完成",
		},
		{
			Id:   15103111039,
			Desc: "推送",
		},
		{
			Id:   15103117235,
			Desc: "完成",
		},
		{
			Id:   15103111039,
			Desc: "完成",
		},
	}
	return result
}

func (o OrderStep) String() string {
	b, err := json.Marshal(o)
	if err != nil {
		panic(fmt.Sprintf("Failed to get order step string, err: %v", err))
	}
	return string(b)
}

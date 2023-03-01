package examples

import (
	"context"
	"github.com/apache/rocketmq-client-go/v2/admin"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"log"
)

func CreateTopicIfNotExist(ctx context.Context, nameserver []string, broker, topic string) {
	a, err := admin.NewAdmin(admin.WithResolver(primitive.NewPassthroughResolver(nameserver)))
	if err != nil {
		log.Printf("Failed to new admin, error: %v\n", err)
	}
	err = a.CreateTopic(
		ctx,
		admin.WithTopicCreate(topic),
		admin.WithBrokerAddrCreate(broker),
	)
	if err != nil {
		log.Printf("Failed to create topic, error: %v\n", err)
	}
}

func DeleteTopic(ctx context.Context, nameserver []string, broker, topic string) {
	a, err := admin.NewAdmin(admin.WithResolver(primitive.NewPassthroughResolver(nameserver)))
	if err != nil {
		log.Printf("Failed to new admin, error: %v\n", err)
	}
	err = a.DeleteTopic(
		ctx,
		admin.WithBrokerAddrDelete(broker),
		admin.WithTopicDelete(topic),
	)
	if err != nil {
		log.Printf("Failed to delete topic, error: %v\n", err)
	}
}

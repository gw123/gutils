package main

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	kafkago "github.com/segmentio/kafka-go"
)

func TestNormalTopicManager(t *testing.T) {
	var count int32
	manager := NewConsumer(ConsumerConfig{
		Brokers: []string{"127.0.0.1:3831", "127.0.0.1:3832", "127.0.0.1:3833"},
		GroupID: "log",
		Topic:   "boss-log10p",
		HandleMassage: func(m *kafkago.Message) error {
			atomic.AddInt32(&count, 1)
			if m.Offset%1000 == 0 {
				fmt.Printf("message at topic:%v partition:%v offset:%v	%s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
			}
			if count%1000 == 0 {
				fmt.Printf("count:%v	\n", count)
			}
			return nil
		},
	})

	if err := manager.Start(context.Background()); err != nil {
		panic(err)
	}

	manager.Close()
}

func TestNormalTopicManagerDev(t *testing.T) {
	var count int32
	manager := NewConsumer(ConsumerConfig{
		Brokers: []string{"kafka-1.neibu.koolearn.com:10193", "kafka-2.neibu.koolearn.com:10193", "kafka-3.neibu.koolearn.com:10193"},
		Topic:   "eccp-signal-for-ai",
		GroupID: "log",
		HandleMassage: func(m *kafkago.Message) error {
			atomic.AddInt32(&count, 1)
			if m.Offset%1000 == 0 {
				fmt.Printf("message at topic:%v partition:%v offset:%v	%s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
			}
			if count%1000 == 0 {
				fmt.Printf("count:%v	\n", count)
			}
			return nil
		},
	})

	if err := manager.Start(context.Background()); err != nil {
		panic(err)
	}
}

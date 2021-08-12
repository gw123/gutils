package main

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/gw123/glog"

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

func TestConsumerOffsetManager(t *testing.T) {
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:       []string{"127.0.0.1:3831", "127.0.0.1:3832", "127.0.0.1:3833"},
		GroupID:       "c1",
		Topic:         "test_offset",
		MinBytes:      1,
		MaxBytes:      1000,
		QueueCapacity: 1,
	})

	ctx := context.Background()

	glog.DefaultLogger().Infof("reader offset %d", reader.Stats().Offset)
	m1, err := reader.FetchMessage(ctx)
	glog.DefaultLogger().Infof("m1 offset %d", m1.Offset, string(m1.Value), err)

	m2, err := reader.FetchMessage(ctx)
	glog.DefaultLogger().Infof("m2 offset %d", m2.Offset, string(m1.Value), err)
	reader.CommitMessages(ctx, m2)

	m3, err := reader.FetchMessage(ctx)
	glog.DefaultLogger().Infof("m3 offset %d", m3.Offset, string(m1.Value), err)

	glog.DefaultLogger().Infof("reader offset %d", reader.Stats().Offset)
}

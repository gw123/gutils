package main

import (
	"context"

	"github.com/gw123/glog"
	"github.com/gw123/glog/common"
	"github.com/pkg/errors"
	kafkago "github.com/segmentio/kafka-go"
)

type KafkaProductor interface {
	GetTopic() string
	Send(ctx context.Context, msgs ...kafkago.Message) error
}

type ProductorConfig struct {
	Brokers []string `json:"brokers"`
	Topic   string   `json:"topic"`
	Log     common.Logger
}

type NormalProductorManager struct {
	topic string
	log   common.Logger
	write *kafkago.Writer
}

func NewNormalProductorManager(config ProductorConfig) KafkaProductor {
	write := &kafkago.Writer{
		Addr:     kafkago.TCP(config.Brokers...),
		Topic:    config.Topic,
		Balancer: &kafkago.LeastBytes{},
	}

	if config.Log == nil {
		config.Log = glog.DefaultLogger()
	}

	return &NormalProductorManager{
		topic: config.Topic,
		log:   config.Log,
		write: write,
	}
}

func (n *NormalProductorManager) GetTopic() string {
	return n.topic
}

func (n *NormalProductorManager) Send(ctx context.Context, msgs ...kafkago.Message) error {
	if err := n.write.WriteMessages(ctx, msgs...); err != nil {
		return errors.Wrap(err, "WriteMessages")
	}
	return nil
}

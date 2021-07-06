package main

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/gw123/glog"
	"github.com/gw123/glog/common"
	kafkago "github.com/segmentio/kafka-go"
)

type KafkaConsumer interface {
	GetTopic() string
	Start(ctx context.Context) error
	End() error
}

type ConsumerConfig struct {
	Brokers       []string `json:"brokers"`
	GroupID       string   `json:"group_id"`
	Topic         string   `json:"topic"`
	HandleMassage func(message *kafkago.Message) error
	Log           common.Logger
}

type Consumer struct {
	topic           string
	reader          *kafkago.Reader
	handleMassage   func(message *kafkago.Message) error
	log             common.Logger
	limit           int // todo 消息限流
	toCommitMessage chan kafkago.Message
}

func NewConsumer(config ConsumerConfig) KafkaConsumer {
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:       config.Brokers,
		GroupID:       config.GroupID,
		Topic:         config.Topic,
		MinBytes:      10e3, // 10KB
		MaxBytes:      10e6, // 10MB
		QueueCapacity: 1000,
	})
	if config.Log == nil {
		config.Log = glog.DefaultLogger()
	}
	return &Consumer{
		topic:           config.Topic,
		reader:          reader,
		handleMassage:   config.HandleMassage,
		log:             config.Log,
		toCommitMessage: make(chan kafkago.Message, 500),
	}
}

func (n *Consumer) GetTopic() string {
	return n.topic
}

// 优化批量提交 每次都提交效率慢10-100倍,异步提交保证不阻塞消息消费（可能存在消息被消费了但是没有提交需要保证幂等）
func (n *Consumer) AutoCommitLoop(ctx context.Context) {
	var toCommitMessageBatch []kafkago.Message
	var timer = time.NewTimer(time.Second)
	var lock sync.RWMutex
	for {
		select {
		case msg := <-n.toCommitMessage:
			lock.RLock()
			toCommitMessageBatch = append(toCommitMessageBatch, msg)
			if len(toCommitMessageBatch) > 400 {
				lock.RUnlock()
				lock.Lock()
				if err := n.reader.CommitMessages(ctx, toCommitMessageBatch...); err != nil {
					n.log.Errorf("Consumer CommitMessages err %s", err)
				} else {
					toCommitMessageBatch = toCommitMessageBatch[:0]
				}
				lock.Unlock()
			} else {
				lock.RUnlock()
			}

		case <-timer.C:
			lock.RLock()
			if len(toCommitMessageBatch) > 0 {
				lock.RUnlock()
				lock.Lock()
				if err := n.reader.CommitMessages(ctx, toCommitMessageBatch...); err != nil {
					n.log.Errorf("Consumer CommitMessages err %s", err)
				} else {
					toCommitMessageBatch = toCommitMessageBatch[:0]
				}
				lock.Unlock()
			} else {
				lock.RUnlock()
			}

		}
	}
	return
}

func (n *Consumer) Start(ctx context.Context) error {
	go n.AutoCommitLoop(ctx)
	for {
		//	ReadMessage will automatically commit the offset when called.
		//m, err := n.reader.ReadMessage(context.Background())

		m, err := n.reader.FetchMessage(ctx)
		if err == io.EOF {
			return err
		}

		if err != nil {
			n.log.Errorf("Consumer reader.ReadMessage err %s", err.Error())
			continue
		}

		func() {
			defer func() {
				if data := recover(); data != nil {
					n.log.Errorf("Consumer recover err %+v", data)
				}
			}()
			if err := n.handleMassage(&m); err != nil {
				n.log.Errorf("Consumer handleMassage err %s", err.Error())
			}
		}()
		// 异步批量提交
		n.toCommitMessage <- m
	}
}

func (n *Consumer) End() error {
	return n.reader.Close()
}

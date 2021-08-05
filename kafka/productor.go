package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"sync"
	"time"

	"github.com/segmentio/kafka-go/sasl"

	"github.com/segmentio/kafka-go/sasl/plain"

	"github.com/gw123/glog"
	"github.com/gw123/glog/common"
	"github.com/pkg/errors"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
)

type KafkaProducer interface {
	GetTopic() string
	Close() error
	Send(ctx context.Context, msg ...kafkago.Message) error
	SendMsg(ctx context.Context, msg interface{}) error
	SendMsgWithHeader(ctx context.Context, header map[string]string, msg interface{}) error
	SendAsync(ctx context.Context, msgs ...kafkago.Message) error
}

type ProducerConfig struct {
	Brokers []string `json:"brokers"`
	Topic   string   `json:"topic"`
	Log     common.Logger
	/***
	&kafkago.LeastBytes{}
	&kafkago.Hash{}
	&kafkago.RoundRobin{}
	&kafkago.CRC32Balancer{}
	&kafkago.Murmur2Balancer{}
	*/
	Balancer kafkago.Balancer
	/**
	Gzip   Compression = compress.Gzip
	Snappy Compression = compress.Snappy
	Lz4    Compression = compress.Lz4
	Zstd   Compression = compress.Zstd
	*/
	Compression kafkago.Compression
	Username    string
	Password    string
	SASLType    string
	TLS         *tls.Config
}

type ProducerManager struct {
	topic           string
	log             common.Logger
	write           *kafkago.Writer
	toCommitMessage chan kafkago.Message
	runFlag         bool
	closeWg         sync.WaitGroup
}

func NewProducerManager(config ProducerConfig) KafkaProducer {
	if config.Log == nil {
		config.Log = glog.DefaultLogger()
	}

	if config.Balancer == nil {
		config.Balancer = &kafkago.LeastBytes{}
	}

	var mechanism sasl.Mechanism
	var err error
	if config.Username != "" {
		switch config.SASLType {
		case "sha512":
			mechanism, err = scram.Mechanism(scram.SHA512, config.Username, config.Password)
			if err != nil {
				config.Log.Errorf("scram.Mechanism %v", err)
			}
		case "sha256":
			mechanism, err = scram.Mechanism(scram.SHA256, config.Username, config.Password)
			if err != nil {
				config.Log.Errorf("scram.Mechanism %v", err)
			}
		case "plain":
			mechanism = plain.Mechanism{
				Username: config.Username,
				Password: config.Password,
			}
		}
	}

	transport := &kafkago.Transport{
		SASL: mechanism,
		TLS:  config.TLS,
	}

	write := &kafkago.Writer{
		Addr:        kafkago.TCP(config.Brokers...),
		Topic:       config.Topic,
		Balancer:    config.Balancer,
		BatchBytes:  10e3, // 10KB
		BatchSize:   100,
		Logger:      config.Log,
		ErrorLogger: config.Log,
		//Compression: kafkago.Compression(kafkago.Gzip),
		Compression: config.Compression,
		Transport:   transport,
	}

	p := &ProducerManager{
		topic:           config.Topic,
		log:             config.Log,
		write:           write,
		toCommitMessage: make(chan kafkago.Message, 500),
		runFlag:         true,
	}
	p.autoCommitLoop(context.Background())
	return p
}

func (n *ProducerManager) GetTopic() string {
	return n.topic
}

func (n *ProducerManager) Send(ctx context.Context, msgs ...kafkago.Message) error {
	if err := n.write.WriteMessages(ctx, msgs...); err != nil {
		return errors.Wrap(err, "WriteMessages")
	}
	return nil
}

func (n *ProducerManager) SendAsync(ctx context.Context, msgs ...kafkago.Message) error {
	for _, msg := range msgs {
		n.toCommitMessage <- msg
	}
	return nil
}

func (n *ProducerManager) SendMsg(ctx context.Context, msg interface{}) error {
	if data, err := json.Marshal(msg); err != nil {
		return errors.Wrap(err, "json.Marshal")
	} else {
		n.toCommitMessage <- kafkago.Message{
			Value: data,
		}
	}
	return nil
}

func (n *ProducerManager) SendMsgWithHeader(ctx context.Context, header map[string]string, msg interface{}) error {
	var kHeaders []kafkago.Header
	for key, val := range header {
		kHeaders = append(kHeaders, kafkago.Header{
			Key:   key,
			Value: []byte(val),
		})
	}

	if data, err := json.Marshal(msg); err != nil {
		return errors.Wrap(err, "json.Marshal")
	} else {
		n.toCommitMessage <- kafkago.Message{
			Headers: kHeaders,
			Value:   data,
		}
	}
	return nil
}

// 优化批量提交 每次都提交效率慢10-100倍,异步提交保证不阻塞消息消费（可能存在消息被消费了但是没有提交需要保证幂等）
func (n *ProducerManager) autoCommitLoop(ctx context.Context) {
	var toCommitMessageBatch []kafkago.Message
	var timer = time.NewTimer(time.Millisecond * 250)
	var lock sync.RWMutex

	go func() {
		for n.runFlag {
			select {
			case msg := <-n.toCommitMessage:
				lock.RLock()
				toCommitMessageBatch = append(toCommitMessageBatch, msg)
				if len(toCommitMessageBatch) >= 1000 {
					lock.RUnlock()
					lock.Lock()
					if err := n.write.WriteMessages(ctx, toCommitMessageBatch...); err != nil {
						n.log.Errorf("ProducerManager CommitMessages err %s", err)
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
					if err := n.write.WriteMessages(ctx, toCommitMessageBatch...); err != nil {
						n.log.Errorf("ProducerManager CommitMessages err %s", err)
					} else {
						toCommitMessageBatch = toCommitMessageBatch[:0]
					}
					lock.Unlock()
				} else {
					lock.RUnlock()
				}

			}
		}
	}()

	n.closeWg.Add(1)
	if len(toCommitMessageBatch) > 0 {
		if err := n.write.WriteMessages(ctx, toCommitMessageBatch...); err != nil {
			n.log.Errorf("ProducerManager CommitMessages err %s", err)
		}
	}
	n.closeWg.Done()

	return
}

func (n *ProducerManager) Close() error {
	n.runFlag = false
	n.closeWg.Wait()
	return n.write.Close()
}

package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/gw123/glog"
)

/***
经过测试 kafka-go 发送1000 000 消息用时间 2.3s  ,sarama库用时间14s

下面代码写入的效率就会降低
write := kafkago.NewWriter(kafkago.WriterConfig{
		Brokers:  config.Brokers,
		Topic:    config.Topic,
		Balancer: &kafkago.LeastBytes{},
		//Logger:      config.Log,
		ErrorLogger: config.Log,
	})

批量发送100w消息V1 使用2.53s  v2使用4.7s
*/

const TopicLog = "boss-log10p"

func TestProducerManagerV1(t *testing.T) {
	productor := NewProducerManager(ProducerConfig{
		Brokers: []string{"127.0.0.1:3831", "127.0.0.1:3832", "127.0.0.1:3833"},
		Topic:   TopicLog,
	})

	var batchMsg []kafkago.Message
	var ctx = context.Background()
	start := time.Now()

	for i := 0; i < 1000000; i++ {
		timeStr := time.Now().Format("2006 01-02 15-04-05")
		data := fmt.Sprintf("index[%d] date[%s]", i, timeStr)

		batchMsg = append(batchMsg, kafkago.Message{Value: []byte(data)})
		if len(batchMsg) == 1000 {
			err := productor.Send(ctx, batchMsg...)
			if err != nil {
				glog.DefaultLogger().Errorf("[%d] send msg err %v", err)
				continue
			}
			batchMsg = batchMsg[:0]
		}
		//glog.DefaultLogger().Infof("send msg over partition %d, offset %d", partition , offset)
		//time.Sleep(time.Millisecond)
	}
	glog.Infof("cast %d ms", time.Now().Sub(start)/time.Millisecond)
}

func TestProducerManagerV2(t *testing.T) {
	producer := NewProducerManager(ProducerConfig{
		Brokers: []string{"127.0.0.1:3831", "127.0.0.1:3832", "127.0.0.1:3833"},
		Topic:   TopicLog,
	})

	//var batchMsg []kafkago.Message
	var ctx = context.Background()
	start := time.Now()

	for i := 0; i < 1000000; i++ {
		timeStr := time.Now().Format("2006 01-02 15-04-05")
		data := fmt.Sprintf("index[%d] date[%s]", i, timeStr)

		//batchMsg = append(batchMsg, kafkago.Message{Value: []byte(data)})
		//err := producer.Send(ctx, batchMsg...)
		err := producer.SendMsg(ctx, data)

		if err != nil {
			glog.DefaultLogger().Errorf("[%d] send msg err %v", err)
			continue
		}
		//	batchMsg = batchMsg[:0]
		//glog.DefaultLogger().Infof("send msg over partition %d, offset %d", partition , offset)
		//time.Sleep(time.Millisecond)
	}
	producer.Close()
	glog.Infof("cast %d ms", time.Now().Sub(start)/time.Millisecond)
}

func TestProducerManagerV3(t *testing.T) {
	producer := NewProducerManager(ProducerConfig{
		Brokers:  []string{"127.0.0.1:3831", "127.0.0.1:3832", "127.0.0.1:3833"},
		Topic:    TopicLog,
		Balancer: &kafkago.CRC32Balancer{},
	})

	//var batchMsg []kafkago.Message
	var ctx = context.Background()
	start := time.Now()

	for i := 0; i < 1000000; i++ {
		timeStr := time.Now().Format("2006 01-02 15-04-05")
		data := fmt.Sprintf("index[%d] date[%s]", i, timeStr)

		//batchMsg = append(batchMsg, kafkago.Message{Value: []byte(data)})
		//err := producer.Send(ctx, batchMsg...)
		err := producer.SendAsync(ctx, kafkago.Message{
			Key:   []byte("123"),
			Value: []byte(data),
		})

		if err != nil {
			glog.DefaultLogger().Errorf("[%d] send msg err %v", err)
			continue
		}
		//	batchMsg = batchMsg[:0]
		//glog.DefaultLogger().Infof("send msg over partition %d, offset %d", partition , offset)
		//time.Sleep(time.Millisecond)
	}
	producer.Close()
	glog.Infof("cast %d ms", time.Now().Sub(start)/time.Millisecond)
}

func TestProducerManagerDev(t *testing.T) {
	producer := NewProducerManager(ProducerConfig{
		Brokers: []string{"127.0.0.1:3831", "127.0.0.1:3832", "127.0.0.1:3833"},
		Topic:   "test_offset",
	})

	//var batchMsg []kafkago.Message
	var ctx = context.Background()
	start := time.Now()

	for i := 0; i < 100000; i++ {
		timeStr := time.Now().Format("2006 01-02 15-04-05")
		data := fmt.Sprintf("index[%d] date[%s]", i, timeStr)

		//batchMsg = append(batchMsg, kafkago.Message{Value: []byte(data)})
		//err := producer.Send(ctx, batchMsg...)
		err := producer.SendMsg(ctx, data)

		if err != nil {
			glog.DefaultLogger().Errorf("[%d] send msg err %v", err)
			continue
		}
		//	batchMsg = batchMsg[:0]
		//glog.DefaultLogger().Infof("send msg over partition %d, offset %d", partition , offset)
		//time.Sleep(time.Millisecond)
	}
	producer.Close()
	glog.Infof("cast %d ms", time.Now().Sub(start)/time.Millisecond)
}

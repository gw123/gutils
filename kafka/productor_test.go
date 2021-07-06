package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gw123/glog"
	kafkago "github.com/segmentio/kafka-go"
)

/***
经过测试 kafka-go 发送1000 000 消息用时间 2.3s  ,sarama库用时间14s
*/

const TopicLog = "boss-log"

func TestNewNormalProductorManager(t *testing.T) {
	productor := NewNormalProductorManager(ProductorConfig{
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

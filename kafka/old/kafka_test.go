package old

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/gw123/glog"
)

type ConsumerGroupHandler struct {
}

func (c ConsumerGroupHandler) Setup(session sarama.ConsumerGroupSession) error {
	glog.DefaultLogger().Infof("consumerGroupHandler setup")
	return nil
}

func (c ConsumerGroupHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	glog.DefaultLogger().Infof("consumerGroupHandler cleanup")
	return nil
}

var consumeCount uint32 = 0

func (c ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	glog.DefaultLogger().Infof("ConsumeClaim topic %s,partition %d", claim.Topic(), claim.Partition())

	for msg := range claim.Messages() {
		session.Commit()
		atomic.AddUint32(&consumeCount, 1)
		if consumeCount%1000 == 0 {
			glog.DefaultLogger().Infof("[%d] topic %s,partition %d, offset %d, msg %s", consumeCount, claim.Topic(), claim.Partition(), msg.Offset, string(msg.Value))
		}
	}
	return nil
}

const TopicLog = "boss-log3"

func TestInitConsume(t *testing.T) {
	ctx := context.Background()
	brokers := []string{"127.0.0.1:3831", "127.0.0.1:3832", "127.0.0.1:3833"}
	_, err := InitConsume(ctx, brokers, "log", TopicLog, &ConsumerGroupHandler{})
	if err != nil {
		t.Fatal(err)
		return
	}
	select {}
}

func TestInitProduct(t *testing.T) {
	ctx := context.Background()
	brokers := []string{"127.0.0.1:3831", "127.0.0.1:3832", "127.0.0.1:3833"}
	productor, err := InitProduct(ctx, brokers)
	if err != nil {
		t.Fatal(err)
		return
	}

	start := time.Now()
	var batchMsg []*sarama.ProducerMessage
	for i := 0; i < 1000000; i++ {
		timeStr := time.Now().Format("2006 01-02 15-04-05")
		data := fmt.Sprintf("index[%d] date[%s]", i, timeStr)
		batchMsg = append(batchMsg, &sarama.ProducerMessage{
			Topic: TopicLog,
			Value: sarama.ByteEncoder(data),
		})

		if len(batchMsg) == 1000 {
			err := productor.SendMessages(batchMsg)
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

func TestInitProductAndConsumer(t *testing.T) {
	ctx := context.Background()
	brokers := []string{"127.0.0.1:3831", "127.0.0.1:3832", "127.0.0.1:3833"}
	_, err := InitConsume(ctx, brokers, "log", TopicLog, &ConsumerGroupHandler{})
	if err != nil {
		t.Fatal(err)
		return
	}

	productor, err := InitProduct(ctx, brokers)
	if err != nil {
		t.Fatal(err)
		return
	}

	go func() {
		start := time.Now()
		for i := 0; i < 10000; i++ {
			timeStr := time.Now().Format("2006 01-02 15-04-05")
			data := fmt.Sprintf("index[%d] date[%s]", i, timeStr)
			partition, offset, err := productor.SendMessage(&sarama.ProducerMessage{
				Topic: TopicLog,
				Value: sarama.ByteEncoder(data),
			})
			if err != nil {
				glog.DefaultLogger().Errorf("send msg err partition %d,offset %d, err %v", partition, offset, err)
				continue
			}
			//glog.DefaultLogger().Infof("send msg over partition %d, offset %d", partition , offset)
			//time.Sleep(time.Millisecond)
		}
		glog.Infof("cast %d ms", time.Now().Sub(start)/time.Millisecond)
	}()

	select {}
}

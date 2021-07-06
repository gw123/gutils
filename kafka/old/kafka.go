package old

import (
	"context"
	"github.com/gw123/glog"
	"github.com/pkg/errors"
	"log"

	"github.com/Shopify/sarama"

	"time"
)

/**

 */
type KafkaConsumer interface {
	SetConsumerHandler(group, topic string)
}

func InitConsume(ctx context.Context, brokers []string, groupName, topicName string, handler sarama.ConsumerGroupHandler) (sarama.ConsumerGroup, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V0_11_0_2 // specify appropriate version
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	//config.Net.SASL.User = "gw"
	//config.Net.SASL.Password = "123456"

	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		log.Println("client create error")
		return nil, err
	}

	offsetManager, err := sarama.NewOffsetManagerFromClient(groupName, client)
	if err != nil {
		log.Println("offsetManager create error")
		return nil, err
	}

	partitions, err := client.Partitions(topicName)
	if err != nil {
		log.Println("offsetManager create error")
		return nil, errors.Wrap(err, "Partitions")
	}

	for _, partition := range partitions {
		partitionOffsetManager, err := offsetManager.ManagePartition(topicName, partition)
		if err != nil {
			return nil, errors.Wrap(err, "ManagePartition")
		}
		next, md := partitionOffsetManager.NextOffset()
		glog.DefaultLogger().Infof("partition %d ,next %d , md %s", partition, next, md)
		group, err := sarama.NewConsumerGroup(brokers, groupName, config)
		if err != nil {
			return nil, err
		}

		go func() {
			for {
				topics := []string{topicName}
				err := group.Consume(ctx, topics, handler)
				if err != nil {
					glog.DefaultLogger().Error("group.Consume: ", err)
				}
			}
		}()
	}

	return nil, nil
}

func InitProduct(ctx context.Context, brokers []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.Timeout = 5 * time.Second
	config.Producer.Partitioner = sarama.NewHashPartitioner
	config.Producer.Retry.Max = 4
	config.Version = sarama.V2_0_0_0

	p, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, errors.Wrap(err, "sarama.NewSyncProducer")
	}
	return p, nil
}

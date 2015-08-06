package rsekafka

import (
	"github.com/Shopify/sarama"

	"fmt"
)

func getProducer(brokers *[]string) sarama.SyncProducer {
	fmt.Println("Getting producer.")
	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, nil)
	if err != nil {
		panic(err)
	}
	return producer
}

/*
func getConsumer(brokers *[]string) sarama.NewConsumer {
	// get Kafka consumer
}
*/

//----------------------------------------------------------------------------------

type Client struct {
	producer sarama.SyncProducer
	//	consumer
}

func NewClient(brokers []string) *Client {
	fmt.Println("Creating client")
	return &Client{
		producer: getProducer(&brokers),
		//		consumer: getConsumer(&brokers),
	}
}

func (c *Client) Post(topic string, message string) error {
	msg := &sarama.ProducerMessage{Topic: topic, Value: sarama.StringEncoder(message)}
	partition, offset, err := c.producer.SendMessage(msg)

	if err != nil {
		fmt.Println("PANIC!!!", err)
	} else {
		fmt.Println("Message saved at", partition, offset)
	}
	return err
}

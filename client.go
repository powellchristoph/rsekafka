package rsekafka

import (
	"github.com/Shopify/sarama"

	"fmt"
)

func getProducer(client sarama.Client) sarama.SyncProducer {
	//fmt.Println("Getting producer.")
	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		panic(err)
	}
	return producer
}

func getMasterConsumer(client sarama.Client) sarama.Consumer {
	//fmt.Println("Getting consumer.")
	master, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		panic(err)
	}
	return master
}

func getClient(brokers []string) sarama.Client {
	client, err := sarama.NewClient(brokers, nil)
	if err != nil {
		panic(err)
	}
	return client
}

//----------------------------------------------------------------------------------

type Client struct {
	client         sarama.Client
	producer       sarama.SyncProducer
	masterConsumer sarama.Consumer
}

func NewClient(brokers []string) *Client {
	//fmt.Println("Creating client")
	client := getClient(brokers)
	return &Client{
		client:         client,
		producer:       getProducer(client),
		masterConsumer: getMasterConsumer(client),
	}
}

// This should return an error
func (c *Client) Close() {
	if err := c.masterConsumer.Close(); err != nil {
		fmt.Println(err)
	}
	if err := c.producer.Close(); err != nil {
		fmt.Println(err)
	}
}

func (c *Client) Post(topic string, message string) error {
	// TODO: Should use the hashparitioner and set a key on the message
	msg := &sarama.ProducerMessage{Topic: topic, Value: sarama.StringEncoder(message)}
	partition, offset, err := c.producer.SendMessage(msg)

	if err != nil {
		fmt.Println("PANIC!!!", err)
	} else {
		fmt.Println("Message saved at", partition, offset)
	}
	return err
}

func (c *Client) Get(topic string, offset int64) ([]*sarama.ConsumerMessage, error) {
	var limit int64 = 100
	consumer, err := c.masterConsumer.ConsumePartition(topic, 0, offset)

	messages := make([]*sarama.ConsumerMessage, 0)

	lastOffset, _ := c.client.GetOffset(topic, 0, sarama.OffsetNewest)
	//fmt.Println("LOS:", lastOffset)
	if lastOffset < limit {
		limit = lastOffset
	}

	// this isn't great.
	var counter int64 = 0
	for msg := range consumer.Messages() {
		//fmt.Println("Found msg", string(msg.Value))
		messages = append(messages, msg)
		counter++
		//fmt.Println("COunter:", counter)
		if counter >= limit {
			//fmt.Println("Breaking")
			break
		}
	}

	consumer.AsyncClose()
	return messages, err
}

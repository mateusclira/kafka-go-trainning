package main

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	deliveryChannel := make(chan kafka.Event)
	producer := NewKafkaProducer()
	Publish("transferencia", "teste", producer, []byte("transferencia2"), deliveryChannel) // key, value, topic, producer, channel
	go DeliveryReport(deliveryChannel)                                                     // forma ASSÍNCRONA

	producer.Flush(4500)
	// e := <-deliveryChannel
	// msg := e.(*kafka.Message)
	// if msg.TopicPartition.Error != nil {
	// 	fmt.Println("Erro ao enviar mensagem") // forma SÍNCRONA
	// } else {
	// 	fmt.Println("Mensagem enviada:", msg.TopicPartition)
	// }

	//
}

func NewKafkaProducer() *kafka.Producer {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers":   "kafka-go-trainning-kafka-1:9092",
		"delivery.timeout.ms": "0",
		"acks":                "all",  // garantir que a mensagem seja enviada para todos os brokers
		"enable.idempotence":  "true", // garantir que a mensagem seja enviada apenas uma vez (e na ordem correta
	}
	p, err := kafka.NewProducer(configMap)
	if err != nil {
		log.Println(err.Error())
	}
	return p
}

func Publish(msg string, topic string, producer *kafka.Producer, key []byte, deliveryChannel chan kafka.Event) error {
	message := kafka.Message{
		Value:          []byte(msg),
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            key,
	}
	err := producer.Produce(&message, deliveryChannel)
	if err != nil {
		return err
	}
	return nil
}

func DeliveryReport(deliveryChannel chan kafka.Event) {
	for e := range deliveryChannel {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				fmt.Println("Erro ao enviar mensagem")
			} else {
				fmt.Println("Mensagem enviada:", ev.TopicPartition)
			}
		}
	}
}

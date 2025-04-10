package main

import (
	"confluent_avro/internal/constants"
	"confluent_avro/internal/models"
	"log"
	"os"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	"github.com/hamba/avro/v2"
	"github.com/riferrei/srclient"
)

func main() {
	// Initialize Schema Registry client
	schemaRegistryClient := srclient.CreateSchemaRegistryClient("http://localhost:8081")
	schemaRegistryClient.CachingEnabled(true)

	// Read and register schema
	schema, err := schemaRegistryClient.GetLatestSchema(constants.Topic)

	if err != nil {
		log.Println("[warn] GetLatestSchema error: ", err)
	}
	if schema == nil {
		schema, err = schemaRegistryClient.CreateSchema(constants.Topic, readSchemaFile("internal/avsc/message.avsc"), srclient.Avro)
		if err != nil {
			log.Fatal(err)
		}
	}

	// Create Kafka producer
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"acks":              "all",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer p.Close()

	// Create message
	msg := models.Message{
		ID:      uuid.NewString(),
		Content: "Hello, Avro!",
		// Timestamp: time.Now().Unix(),
	}

	// Create Avro codec
	codec, err := avro.Parse(schema.Schema())
	if err != nil {
		log.Fatal(err)
	}

	// Serialize message
	msgBinary, err := avro.Marshal(codec, msg)
	if err != nil {
		log.Fatal(err)
	}

	// Create message payload with schema ID
	var payload []byte
	payload = append(payload, byte(0))               // Magic byte
	payload = append(payload, byte(schema.ID()>>24)) // Schema ID
	payload = append(payload, byte(schema.ID()>>16))
	payload = append(payload, byte(schema.ID()>>8))
	payload = append(payload, byte(schema.ID()))
	payload = append(payload, msgBinary...)

	// Produce message
	deliveryChan := make(chan kafka.Event)
	err = p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &[]string{constants.Topic}[0],
			Partition: kafka.PartitionAny,
		},
		Value: payload,
	}, deliveryChan)

	if err != nil {
		log.Fatal(err)
	}

	// Wait for delivery report
	e := <-deliveryChan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		log.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
	} else {
		log.Printf("Delivered message to topic %s [%d] at offset %v\n",
			*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
	}

	p.Flush(15 * 1000)
}

func readSchemaFile(filepath string) string {
	schemaBytes, err := os.ReadFile(filepath)
	if err != nil {
		log.Fatal(err)
	}
	return string(schemaBytes)
}

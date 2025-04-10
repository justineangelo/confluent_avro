package main

import (
	"confluent_avro/internal/constants"
	"confluent_avro/internal/models"
	"encoding/binary"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/hamba/avro/v2"
	"github.com/riferrei/srclient"
)

func main() {
	// Initialize Schema Registry client
	schemaRegistryClient := srclient.CreateSchemaRegistryClient("http://localhost:8081")
	schemaRegistryClient.CachingEnabled(true)

	// Create Kafka consumer
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "test-group1",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	// Subscribe to topic
	c.SubscribeTopics([]string{constants.Topic}, nil)

	log.Println("Consumer server started...")
	for {
		msg, err := c.ReadMessage(-1)
		if err != nil {
			log.Printf("Consumer error: %v\n", err)
			continue
		}

		// Extract schema ID and payload
		schemaID := binary.BigEndian.Uint32(msg.Value[1:5])
		payload := msg.Value[5:]

		// Get schema from registry
		schema, err := schemaRegistryClient.GetSchema(int(schemaID))
		if err != nil {
			log.Printf("Error getting schema: %v\n", err)
			continue
		}

		// Parse schema
		codec, err := avro.Parse(schema.Schema())
		if err != nil {
			log.Printf("Error parsing schema: %v\n", err)
			continue
		}

		// Unmarshal message
		var message models.Message
		err = avro.Unmarshal(codec, payload, &message)
		if err != nil {
			log.Fatal(err)
		}

		log.Printf("Received message: %+v\n", message)
	}
}

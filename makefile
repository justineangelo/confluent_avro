up:
	docker compose up -d

run_producer:
	go run cmd/producer/main.go

run_consumer:
	go run cmd/consumer/main.go

avro_gen:
	avrogen -pkg avro -o internal/models/models.go -pkg models -tags json:snake,yaml:upper-camel internal/avsc/message.avsc

package com.CRUDKafka.CRUDKafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.util.Arrays;
import java.util.UUID;

@Component
public class ProductController {
    private static final Logger logger = LoggerFactory.getLogger(ProductController.class);
    private final ProductService service;
    private final ObjectMapper objectMapper;
    private final KafkaSender<String, String> kafkaSender;
    private final ReceiverOptions<String, String> receiverOptions;

    public ProductController(ProductService service, ObjectMapper objectMapper) {
        this.service = service;
        this.objectMapper = objectMapper;

        // Kafka Producer Setup
        SenderOptions<String, String> senderOptions = SenderOptions.<String, String>create()
            .producerProperty("bootstrap.servers", "localhost:9092")
            .producerProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
            .producerProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        this.kafkaSender = KafkaSender.create(senderOptions);

        // Kafka Consumer Setup
        this.receiverOptions = ReceiverOptions.<String, String>create()
            .consumerProperty("bootstrap.servers", "localhost:9092")
            .consumerProperty("group.id", "product-group")
            .consumerProperty("auto.offset.reset", "earliest")
            .consumerProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
            .consumerProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
            .subscription(Arrays.asList("product-create", "product-get", "product-update", "product-delete", "product-get-all"));
    }

    @jakarta.annotation.PostConstruct
    public void init() {
        reactor.kafka.receiver.KafkaReceiver.create(receiverOptions)
            .receive()
            .publishOn(Schedulers.boundedElastic()) // I/O tasks
            .flatMap(this::dispatchMessage, 100) // Process 100 messages concurrently
            .doOnNext(response -> kafkaSender.send(Mono.just(
                    SenderRecord.create(new org.apache.kafka.clients.producer.ProducerRecord<>("product-response", null, null, response), null)))
                .doOnError(e -> logger.error("Failed to send response", e))
                .subscribe())
            .doOnError(e -> logger.error("Error in Kafka pipeline", e))
            .subscribe();
    }

    private Mono<String> dispatchMessage(ReceiverRecord<String, String> record) {
        String correlationId = record.headers().lastHeader("correlationId") != null
            ? new String(record.headers().lastHeader("correlationId").value())
            : UUID.randomUUID().toString();
        String topic = record.topic();
        logger.info("Processing message from topic: {}, correlationId: {}", topic, correlationId);

        return switch (topic) {
            case "product-create" -> processCreate(record, correlationId);
            case "product-get" -> processGet(record, correlationId);
            case "product-get-all" -> processGetAll(record, correlationId);
            case "product-update" -> processUpdate(record, correlationId);
            case "product-delete" -> processDelete(record, correlationId);
            default -> Mono.empty(); // Skip unknown topics
        };
    }

    private Mono<String> processCreate(ReceiverRecord<String, String> record, String correlationId) {
        return Mono.fromCallable(() -> objectMapper.readValue(record.value(), Product.class))
            .publishOn(Schedulers.parallel()) // CPU-bound parsing
            .flatMap(product -> {
                logger.info("Creating product ID: {}, correlationId: {}", product.getId(), correlationId);
                return service.create(product)
                    .map(result -> toSuccessJson(result, "CREATE", correlationId));
            })
            .doOnSuccess(s -> record.receiverOffset().acknowledge())
            .doOnError(e -> logger.error("Failed to process CREATE: {}", e.getMessage()));
    }

    private Mono<String> processGet(ReceiverRecord<String, String> record, String correlationId) {
        return Mono.fromCallable(() -> objectMapper.readValue(record.value(), String.class))
            .publishOn(Schedulers.parallel())
            .flatMap(id -> {
                logger.info("Retrieving product ID: {}, correlationId: {}", id, correlationId);
                return service.getById(id)
                    .map(result -> toSuccessJson(result, "GET", correlationId));
            })
            .doOnSuccess(s -> record.receiverOffset().acknowledge())
            .doOnError(e -> logger.error("Failed to process GET: {}", e.getMessage()));
    }

    private Mono<String> processGetAll(ReceiverRecord<String, String> record, String correlationId) {
        return Mono.just("")
            .publishOn(Schedulers.parallel())
            .flatMap(ignored -> {
                logger.info("Retrieving all products, correlationId: {}", correlationId);
                return service.getAll()
                    .collectList()
                    .map(result -> toSuccessJson(result, "GET_ALL", correlationId));
            })
            .doOnSuccess(s -> record.receiverOffset().acknowledge())
            .doOnError(e -> logger.error("Failed to process GET_ALL: {}", e.getMessage()));
    }

    private Mono<String> processUpdate(ReceiverRecord<String, String> record, String correlationId) {
        return Mono.fromCallable(() -> objectMapper.readValue(record.value(), Product.class))
            .publishOn(Schedulers.parallel())
            .flatMap(product -> {
                logger.info("Updating product ID: {}, correlationId: {}", product.getId(), correlationId);
                return service.update(product.getId(), product)
                    .map(result -> toSuccessJson(result, "UPDATE", correlationId));
            })
            .doOnSuccess(s -> record.receiverOffset().acknowledge())
            .doOnError(e -> logger.error("Failed to process UPDATE: {}", e.getMessage()));
    }

    private Mono<String> processDelete(ReceiverRecord<String, String> record, String correlationId) {
        return Mono.fromCallable(() -> objectMapper.readValue(record.value(), String.class))
            .publishOn(Schedulers.parallel())
            .flatMap(id -> {
                logger.info("Deleting product ID: {}, correlationId: {}", id, correlationId);
                return service.delete(id)
                    .thenReturn(new SuccessResponse("DELETE", "Deleted: " + id, correlationId))
                    .map(this::toJson);
            })
            .doOnSuccess(s -> record.receiverOffset().acknowledge())
            .doOnError(e -> logger.error("Failed to process DELETE: {}", e.getMessage()));
    }

    private String toSuccessJson(Object result, String operation, String correlationId) {
        try {
            return objectMapper.writeValueAsString(
                new SuccessResponse(operation, result, correlationId)
            );
        } catch (Exception e) {
            throw new RuntimeException("Serialization failed", e);
        }
    }

    private String toJson(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (Exception e) {
            throw new RuntimeException("Serialization failed", e);
        }
    }

    static class SuccessResponse {
        private String operation;
        private Object result;
        private String correlationId;

        public SuccessResponse(String operation, Object result, String correlationId) {
            this.operation = operation;
            this.result = result;
            this.correlationId = correlationId;
        }

        public String getOperation() { return operation; }
        public void setOperation(String operation) { this.operation = operation; }
        public Object getResult() { return result; }
        public void setResult(Object result) { this.result = result; }
        public String getCorrelationId() { return correlationId; }
        public void setCorrelationId(String correlationId) { this.correlationId = correlationId; }
    }
}
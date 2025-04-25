
package com.CRUDGateway;

import com.CRUDGateway.Product;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class KafkaGatewayService {
    private static final Logger logger = LoggerFactory.getLogger(KafkaGatewayService.class);
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final Map<String, String> responseStore = new ConcurrentHashMap<>();
    private static final int MAX_RETRIES = 20; // 2 seconds total (20 * 100ms)
    private static final Duration RETRY_DELAY = Duration.ofMillis(100);

    public KafkaGatewayService(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "product-response", groupId = "gateway-group")
    public void consumeResponse(ConsumerRecord<String, String> record) {
        try {
            String message = record.value();
            logger.info("Received response: {}", message);
            Map<String, Object> response = objectMapper.readValue(message, Map.class);
            String correlationId = (String) response.get("correlationId");
            if (correlationId != null) {
                responseStore.put(correlationId, message);
                logger.info("Stored response for correlationId: {}", correlationId);
            } else {
                logger.warn("No correlationId in response: {}", message);
            }
        } catch (Exception e) {
            logger.error("Failed to process response", e);
        }
    }

    public Mono<String> sendCreate(Product product) {
        return sendRequest("product-create", toJson(product));
    }

    public Mono<String> sendGet(String id) {
        return sendRequest("product-get", "\"" + id + "\"");
    }

    public Mono<String> sendGetAll() {
        return sendRequest("product-get-all", "");
    }

    public Mono<String> sendUpdate(Product product) {
        return sendRequest("product-update", toJson(product));
    }

    public Mono<String> sendDelete(String id) {
        return sendRequest("product-delete", "\"" + id + "\"");
    }

    public Mono<String> sendBulk(int count) {
        return Mono.fromCallable(() -> {
            long startTime = System.currentTimeMillis();
            for (int i = 0; i < count; i++) {
                String id = String.valueOf(i);
                Product product = new Product();
                product.setId(id);
                product.setName("Product" + id);
                product.setQuantity(10);
                product.setPrice(999.99);

                sendCreate(product).subscribe();
                sendGet(id).subscribe();
                sendUpdate(product).subscribe();
                sendDelete(id).subscribe();
                sendGetAll().subscribe();
            }
            long endTime = System.currentTimeMillis();
            return "Sent " + (count * 5) + " messages in " + (endTime - startTime) + " ms";
        }).subscribeOn(Schedulers.boundedElastic());
    }

    private Mono<String> sendRequest(String topic, String message) {
        String correlationId = UUID.randomUUID().toString();
        return Mono.fromCallable(() -> {
            org.apache.kafka.clients.producer.ProducerRecord<String, String> record =
                new org.apache.kafka.clients.producer.ProducerRecord<>(topic, message);
            record.headers().add("correlationId", correlationId.getBytes());
            kafkaTemplate.send(record).get();
            logger.info("Sent request to {} with correlationId: {}", topic, correlationId);
            return correlationId;
        })
            .subscribeOn(Schedulers.boundedElastic())
            .flatMap(id -> Mono.defer(() -> {
                String response = responseStore.remove(id);
                if (response != null) {
                    return Mono.just(response);
                }
                return Mono.<String>empty();
            })
                .repeatWhenEmpty(MAX_RETRIES, repeat -> repeat.delayElements(RETRY_DELAY))
                .switchIfEmpty(Mono.error(new RuntimeException("No response received for correlationId: " + id))));
    }

    private String toJson(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (Exception e) {
            throw new RuntimeException("Serialization failed", e);
        }
    }
}

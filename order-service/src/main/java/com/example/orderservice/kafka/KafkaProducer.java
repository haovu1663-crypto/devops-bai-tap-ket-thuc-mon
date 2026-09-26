package com.example.orderservice.kafka;

import com.example.orderservice.dto.OrderEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class KafkaProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    private static final String TOPIC = "order-created";

    public void sendOrderCreatedEvent(OrderEvent event) {
        log.info(">>> Sending OrderEvent to topic '{}': {}", TOPIC, event);
        kafkaTemplate.send(TOPIC, String.valueOf(event.getOrderId()), event);
    }
}

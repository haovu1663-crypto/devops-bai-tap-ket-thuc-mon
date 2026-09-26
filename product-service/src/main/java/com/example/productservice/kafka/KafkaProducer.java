package com.example.productservice.kafka;

import com.example.productservice.dto.ProductReservedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class KafkaProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    private static final String TOPIC = "product-reserved";

    public void sendProductReservedEvent(ProductReservedEvent event) {
        log.info(">>> Sending ProductReservedEvent to topic '{}': {}", TOPIC, event);
        kafkaTemplate.send(TOPIC, String.valueOf(event.getOrderId()), event);
    }
}

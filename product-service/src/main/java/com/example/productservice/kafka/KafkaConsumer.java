package com.example.productservice.kafka;

import com.example.productservice.dto.OrderEvent;
import com.example.productservice.service.ProductService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class KafkaConsumer {

    private final ProductService productService;

    @KafkaListener(topics = "order-created", groupId = "product-service-group")
    public void consumeOrderCreatedEvent(OrderEvent event) {
        log.info("<<< Received OrderEvent from topic 'order-created': {}", event);
        productService.handleOrderCreated(event);
    }
}

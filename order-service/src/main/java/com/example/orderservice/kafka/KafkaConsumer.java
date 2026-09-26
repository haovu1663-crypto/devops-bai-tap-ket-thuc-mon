package com.example.orderservice.kafka;

import com.example.orderservice.dto.ProductReservedEvent;
import com.example.orderservice.service.OrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class KafkaConsumer {

    private final OrderService orderService;

    @KafkaListener(topics = "product-reserved", groupId = "order-service-group")
    public void consumeProductReservedEvent(ProductReservedEvent event) {
        log.info("<<< Received ProductReservedEvent from topic 'product-reserved': {}", event);
        orderService.handleProductReserved(event);
    }
}

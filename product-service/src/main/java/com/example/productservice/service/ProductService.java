package com.example.productservice.service;

import com.example.productservice.dto.OrderEvent;
import com.example.productservice.dto.ProductReservedEvent;
import com.example.productservice.kafka.KafkaProducer;
import com.example.productservice.model.Product;
import com.example.productservice.repository.ProductRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;

@Service
@RequiredArgsConstructor
@Slf4j
public class ProductService {

    private final ProductRepository productRepository;
    private final KafkaProducer kafkaProducer;

    public List<Product> getAllProducts() {
        return productRepository.findAll();
    }

    public Optional<Product> getProductById(Long id) {
        return productRepository.findById(id);
    }

    public Product createProduct(Product product) {
        log.info("Creating product: {}", product.getName());
        return productRepository.save(product);
    }

    /**
     * Xử lý sự kiện order-created từ Kafka:
     * - Kiểm tra sản phẩm có tồn tại không
     * - Kiểm tra tồn kho có đủ không
     * - Trừ tồn kho nếu đủ
     * - Gửi event product-reserved (CONFIRMED/FAILED)
     */
    @Transactional
    public void handleOrderCreated(OrderEvent orderEvent) {
        log.info("Processing order for productId={}, quantity={}",
                orderEvent.getProductId(), orderEvent.getQuantity());

        Optional<Product> optionalProduct = productRepository.findById(orderEvent.getProductId());

        if (optionalProduct.isEmpty()) {
            // Sản phẩm không tồn tại
            log.warn("Product not found with id={}", orderEvent.getProductId());
            ProductReservedEvent event = new ProductReservedEvent(
                    orderEvent.getOrderId(),
                    orderEvent.getProductId(),
                    "FAILED",
                    "Product not found with id=" + orderEvent.getProductId()
            );
            kafkaProducer.sendProductReservedEvent(event);
            return;
        }

        Product product = optionalProduct.get();

        if (product.getQuantity() >= orderEvent.getQuantity()) {
            // Đủ hàng → trừ tồn kho & xác nhận
            product.setQuantity(product.getQuantity() - orderEvent.getQuantity());
            productRepository.save(product);
            log.info("Product reserved successfully. Remaining quantity: {}", product.getQuantity());

            ProductReservedEvent event = new ProductReservedEvent(
                    orderEvent.getOrderId(),
                    orderEvent.getProductId(),
                    "CONFIRMED",
                    "Product reserved successfully. Remaining stock: " + product.getQuantity()
            );
            kafkaProducer.sendProductReservedEvent(event);
        } else {
            // Không đủ hàng
            log.warn("Insufficient stock for productId={}. Available: {}, Requested: {}",
                    product.getId(), product.getQuantity(), orderEvent.getQuantity());

            ProductReservedEvent event = new ProductReservedEvent(
                    orderEvent.getOrderId(),
                    orderEvent.getProductId(),
                    "FAILED",
                    "Insufficient stock. Available: " + product.getQuantity()
                            + ", Requested: " + orderEvent.getQuantity()
            );
            kafkaProducer.sendProductReservedEvent(event);
        }
    }
}

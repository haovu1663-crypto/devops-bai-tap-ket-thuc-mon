package com.example.orderservice.service;

import com.example.orderservice.dto.OrderEvent;
import com.example.orderservice.dto.OrderRequest;
import com.example.orderservice.dto.ProductReservedEvent;
import com.example.orderservice.kafka.KafkaProducer;
import com.example.orderservice.model.Order;
import com.example.orderservice.repository.OrderRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.*;

@Service
@RequiredArgsConstructor
@Slf4j
public class OrderService {

    private final OrderRepository orderRepository;
    private final KafkaProducer kafkaProducer;

    // Map lưu CompletableFuture cho các request đang chờ phản hồi từ Kafka
    private final Map<Long, CompletableFuture<ProductReservedEvent>> pendingOrders = new ConcurrentHashMap<>();

    public List<Order> getAllOrders() {
        return orderRepository.findAll();
    }

    public Optional<Order> getOrderById(Long id) {
        return orderRepository.findById(id);
    }

    /**
     * Tạo đơn hàng mới:
     * 1. Validate request (productId, quantity > 0)
     * 2. Gửi event qua Kafka để Product Service kiểm tra số lượng tồn kho & giữ hàng
     * 3. Chờ phản hồi từ Kafka:
     *    - Nếu đủ hàng (CONFIRMED): Tạo đơn hàng vào DB và trả về kết quả 201 CREATED
     *    - Nếu không đủ hàng (FAILED): KHÔNG tạo đơn hàng, trả về lỗi HTTP 400 BAD REQUEST
     */
    public Order createOrder(OrderRequest request) {
        if (request.getProductId() == null) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "ProductId không được để trống");
        }
        if (request.getQuantity() == null || request.getQuantity() <= 0) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Số lượng đặt hàng (quantity) phải lớn hơn 0");
        }

        // Tạo correlationId duy nhất cho giao dịch kiểm tra tồn kho
        long correlationId = System.currentTimeMillis() * 1000 + ThreadLocalRandom.current().nextInt(1000);

        CompletableFuture<ProductReservedEvent> future = new CompletableFuture<>();
        pendingOrders.put(correlationId, future);

        log.info("Sending stock check request via Kafka for productId={}, quantity={}, correlationId={}",
                request.getProductId(), request.getQuantity(), correlationId);

        // Gửi event qua Kafka sang Product Service
        OrderEvent event = new OrderEvent(
                correlationId,
                request.getProductId(),
                request.getQuantity()
        );
        kafkaProducer.sendOrderCreatedEvent(event);

        try {
            // Chờ phản hồi từ Product Service qua Kafka (tối đa 5 giây)
            ProductReservedEvent response = future.get(5, TimeUnit.SECONDS);

            if ("CONFIRMED".equalsIgnoreCase(response.getStatus())) {
                // Tồn kho đủ và đã được Product Service trừ -> Tạo và lưu đơn hàng vào DB
                Order order = new Order();
                order.setProductId(request.getProductId());
                order.setQuantity(request.getQuantity());
                order.setTotalPrice(request.getTotalPrice() != null ? request.getTotalPrice() : 0.0);
                order.setStatus("CONFIRMED");
                order.setMessage(response.getMessage());

                Order savedOrder = orderRepository.save(order);
                log.info("Order successfully created with id={}, status=CONFIRMED", savedOrder.getId());
                return savedOrder;
            } else {
                // Không đủ tồn kho hoặc sản phẩm không tồn tại -> KHÔNG tạo đơn hàng
                log.warn("Stock check failed for correlationId={}: {}", correlationId, response.getMessage());
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, response.getMessage());
            }
        } catch (TimeoutException e) {
            log.error("Timeout waiting for Kafka response for correlationId={}", correlationId);
            throw new ResponseStatusException(HttpStatus.GATEWAY_TIMEOUT,
                    "Product Service không phản hồi kiểm tra tồn kho qua Kafka kịp thời (Timeout 5s)");
        } catch (InterruptedException | ExecutionException e) {
            Thread.currentThread().interrupt();
            log.error("Error waiting for Kafka response for correlationId={}", correlationId, e);
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR,
                    "Lỗi hệ thống khi kiểm tra tồn kho qua Kafka");
        } finally {
            pendingOrders.remove(correlationId);
        }
    }

    /**
     * Xử lý phản hồi từ Product Service qua Kafka:
     * - Tìm CompletableFuture tương ứng với correlationId và hoàn thành nó
     */
    public void handleProductReserved(ProductReservedEvent event) {
        log.info("Received Kafka ProductReservedEvent for correlationId={}, status={}",
                event.getOrderId(), event.getStatus());

        CompletableFuture<ProductReservedEvent> future = pendingOrders.get(event.getOrderId());
        if (future != null) {
            future.complete(event);
        } else {
            // Trường hợp không có future trong bộ nhớ (ví dụ đơn cũ từ trước), cập nhật DB nếu tìm thấy
            orderRepository.findById(event.getOrderId()).ifPresent(order -> {
                order.setStatus(event.getStatus());
                order.setMessage(event.getMessage());
                orderRepository.save(order);
                log.info("Updated existing order id={} to status={}", order.getId(), order.getStatus());
            });
        }
    }
}

# 🚀 Microservices với Kafka - Order & Product Service

Hệ thống microservice gồm 2 service giao tiếp bất đồng bộ qua Apache Kafka.

## 📐 Kiến Trúc

```
Client → Order Service (8082) → Kafka → Product Service (8081) → Kafka → Order Service
```

### Luồng hoạt động:
1. Client gửi `POST /api/orders` tới **Order Service**
2. Order Service lưu đơn hàng (status: `PENDING`) → gửi event `order-created` qua Kafka
3. **Product Service** nhận event → kiểm tra tồn kho → trừ hàng nếu đủ
4. Product Service gửi event `product-reserved` (CONFIRMED/FAILED) qua Kafka
5. Order Service nhận event → cập nhật trạng thái đơn hàng

## 🛠️ Công Nghệ

| Component | Technology |
|-----------|-----------|
| Backend | Spring Boot 3.2 + Java 17 |
| Message Broker | Apache Kafka (Confluent) |
| Database | PostgreSQL 15 |
| Container | Docker & Docker Compose |

## 🚀 Cách Chạy

### Yêu cầu
- Docker & Docker Compose đã cài đặt

### Khởi chạy toàn bộ hệ thống

```bash
docker-compose up --build
```

### Dừng hệ thống

```bash
docker-compose down
```

### Xóa toàn bộ (bao gồm data)

```bash
docker-compose down -v
```

## 📡 API Endpoints

### Product Service - `http://localhost:8081`

| Method | Endpoint | Mô tả |
|--------|----------|--------|
| GET | `/api/products` | Lấy danh sách sản phẩm |
| GET | `/api/products/{id}` | Lấy sản phẩm theo ID |
| POST | `/api/products` | Tạo sản phẩm mới |

### Order Service - `http://localhost:8082`

| Method | Endpoint | Mô tả |
|--------|----------|--------|
| GET | `/api/orders` | Lấy danh sách đơn hàng |
| GET | `/api/orders/{id}` | Lấy đơn hàng theo ID |
| POST | `/api/orders` | Tạo đơn hàng mới |

## 🧪 Test End-to-End

### Bước 1: Tạo sản phẩm

```bash
curl -X POST http://localhost:8081/api/products \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Laptop Dell XPS 15",
    "price": 25000000,
    "quantity": 10,
    "description": "Laptop cao cấp"
  }'
```

### Bước 2: Tạo đơn hàng

```bash
curl -X POST http://localhost:8082/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "productId": 1,
    "quantity": 2,
    "totalPrice": 50000000
  }'
```

### Bước 3: Kiểm tra trạng thái đơn hàng (đợi vài giây)

```bash
curl http://localhost:8082/api/orders/1
```

**Kết quả mong đợi:** `status` chuyển từ `PENDING` → `CONFIRMED`

### Bước 4: Kiểm tra tồn kho đã giảm

```bash
curl http://localhost:8081/api/products/1
```

**Kết quả mong đợi:** `quantity` giảm từ `10` → `8`

## 📂 Cấu Trúc Thư Mục

```
baitapcuoimon/
├── docker-compose.yml
├── README.md
├── product-service/
│   ├── Dockerfile
│   ├── pom.xml
│   └── src/main/java/com/example/productservice/
│       ├── ProductServiceApplication.java
│       ├── config/KafkaConfig.java
│       ├── controller/ProductController.java
│       ├── model/Product.java
│       ├── repository/ProductRepository.java
│       ├── service/ProductService.java
│       ├── kafka/
│       │   ├── KafkaConsumer.java
│       │   └── KafkaProducer.java
│       └── dto/
│           ├── OrderEvent.java
│           └── ProductReservedEvent.java
└── order-service/
    ├── Dockerfile
    ├── pom.xml
    └── src/main/java/com/example/orderservice/
        ├── OrderServiceApplication.java
        ├── config/KafkaConfig.java
        ├── controller/OrderController.java
        ├── model/Order.java
        ├── repository/OrderRepository.java
        ├── service/OrderService.java
        ├── kafka/
        │   ├── KafkaConsumer.java
        │   └── KafkaProducer.java
        └── dto/
            ├── OrderEvent.java
            ├── OrderRequest.java
            └── ProductReservedEvent.java
```

## 🔧 Kafka Topics

| Topic | Producer | Consumer | Mô tả |
|-------|----------|----------|--------|
| `order-created` | Order Service | Product Service | Khi đơn hàng mới được tạo |
| `product-reserved` | Product Service | Order Service | Kết quả kiểm tra tồn kho |

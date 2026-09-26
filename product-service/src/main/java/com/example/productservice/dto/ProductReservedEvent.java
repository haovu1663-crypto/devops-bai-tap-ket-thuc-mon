package com.example.productservice.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ProductReservedEvent {

    private Long orderId;
    private Long productId;
    private String status;  // CONFIRMED or FAILED
    private String message;
}

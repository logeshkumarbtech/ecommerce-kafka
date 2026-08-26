package com.ecommerce.common.event;

public record OrderCreatedEvent(
        String orderId,
        String customerId,
        String productId,
        int quantity,
        double amount
) {}

package com.ecommerce.order.status;

public record OrderStatusView(
        String orderId,
        String customerId,
        String productId,
        int quantity,
        double amount,
        String status,
        String transactionId,
        String trackingNumber,
        String estimatedDelivery,
        String updatedAt
) {}

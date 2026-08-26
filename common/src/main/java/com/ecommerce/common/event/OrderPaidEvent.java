package com.ecommerce.common.event;

public record OrderPaidEvent(
        String orderId,
        String customerId,
        double amount,
        String transactionId
) {}

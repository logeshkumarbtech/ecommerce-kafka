package com.ecommerce.common.event;

public record OrderShippedEvent(
        String orderId,
        String customerId,
        String trackingNumber,
        String estimatedDelivery
) {}

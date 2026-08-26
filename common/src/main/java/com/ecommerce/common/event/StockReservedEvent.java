package com.ecommerce.common.event;

public record StockReservedEvent(
        String orderId,
        String productId,
        int quantity
) {}

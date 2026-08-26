package com.ecommerce.order.dto;

public record OrderRequest(
        String customerId,
        String productId,
        int quantity,
        double amount
) {}

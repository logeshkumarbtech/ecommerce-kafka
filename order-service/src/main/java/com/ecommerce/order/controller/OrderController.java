package com.ecommerce.order.controller;

import com.ecommerce.common.event.OrderCreatedEvent;
import com.ecommerce.order.dto.OrderRequest;
import com.ecommerce.order.producer.OrderProducer;
import com.ecommerce.order.status.OrderStatusStore;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final OrderProducer orderProducer;
    private final OrderStatusStore orderStatusStore;

    public OrderController(OrderProducer orderProducer, OrderStatusStore orderStatusStore) {
        this.orderProducer = orderProducer;
        this.orderStatusStore = orderStatusStore;
    }

    @PostMapping
    public ResponseEntity<Map<String, String>> createOrder(@RequestBody OrderRequest request) {
        String orderId = UUID.randomUUID().toString();

        OrderCreatedEvent event = new OrderCreatedEvent(
                orderId,
                request.customerId(),
                request.productId(),
                request.quantity(),
                request.amount()
        );

        orderStatusStore.registerCreated(event);
        orderProducer.publish(event);
        return ResponseEntity.accepted().body(Map.of("orderId", orderId, "status", "PROCESSING"));
    }
}

package com.ecommerce.order.status;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.Collection;

@RestController
@RequestMapping("/api/orders")
public class OrderStatusController {

    private final OrderStatusStore store;

    public OrderStatusController(OrderStatusStore store) {
        this.store = store;
    }

    @GetMapping
    public Collection<OrderStatusView> listOrders() {
        return store.all();
    }

    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter stream() {
        return store.subscribe();
    }
}

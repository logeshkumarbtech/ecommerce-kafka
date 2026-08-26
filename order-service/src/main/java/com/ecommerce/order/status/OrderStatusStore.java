package com.ecommerce.order.status;

import com.ecommerce.common.event.OrderCreatedEvent;
import com.ecommerce.common.event.OrderPaidEvent;
import com.ecommerce.common.event.OrderShippedEvent;
import com.ecommerce.common.event.StockReservedEvent;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/** In-memory order status board, fed by Kafka events, pushed to browsers via SSE. */
@Component
public class OrderStatusStore {

    private final Map<String, OrderStatusView> orders = new ConcurrentHashMap<>();
    private final List<SseEmitter> emitters = new CopyOnWriteArrayList<>();

    public Collection<OrderStatusView> all() {
        return orders.values();
    }

    public SseEmitter subscribe() {
        SseEmitter emitter = new SseEmitter(30 * 60 * 1000L);
        emitters.add(emitter);
        emitter.onCompletion(() -> emitters.remove(emitter));
        emitter.onTimeout(() -> emitters.remove(emitter));
        emitter.onError(e -> emitters.remove(emitter));
        return emitter;
    }

    public void registerCreated(OrderCreatedEvent event) {
        OrderStatusView view = new OrderStatusView(
                event.orderId(), event.customerId(), event.productId(),
                event.quantity(), event.amount(), "CREATED",
                null, null, null, Instant.now().toString());
        orders.put(event.orderId(), view);
        broadcast(view);
    }

    public void stockReserved(StockReservedEvent event) {
        orders.compute(event.orderId(), (id, existing) ->
                merge(existing, event.orderId(), "STOCK_RESERVED", null, null, null));
    }

    public void orderPaid(OrderPaidEvent event) {
        orders.compute(event.orderId(), (id, existing) ->
                merge(existing, event.orderId(), "PAID", event.transactionId(), null, null));
    }

    public void orderShipped(OrderShippedEvent event) {
        orders.compute(event.orderId(), (id, existing) ->
                merge(existing, event.orderId(), "SHIPPED", null, event.trackingNumber(), event.estimatedDelivery()));
    }

    private OrderStatusView merge(OrderStatusView existing, String orderId, String status,
                                   String transactionId, String trackingNumber, String estimatedDelivery) {
        OrderStatusView updated = existing == null
                ? new OrderStatusView(orderId, null, null, 0, 0,
                        status, transactionId, trackingNumber, estimatedDelivery, Instant.now().toString())
                : new OrderStatusView(existing.orderId(), existing.customerId(), existing.productId(),
                        existing.quantity(), existing.amount(), status,
                        transactionId != null ? transactionId : existing.transactionId(),
                        trackingNumber != null ? trackingNumber : existing.trackingNumber(),
                        estimatedDelivery != null ? estimatedDelivery : existing.estimatedDelivery(),
                        Instant.now().toString());
        broadcast(updated);
        return updated;
    }

    private void broadcast(OrderStatusView view) {
        List<SseEmitter> dead = new ArrayList<>();
        for (SseEmitter emitter : emitters) {
            try {
                emitter.send(SseEmitter.event().name("order-update").data(view));
            } catch (IOException ex) {
                dead.add(emitter);
            }
        }
        emitters.removeAll(dead);
    }
}

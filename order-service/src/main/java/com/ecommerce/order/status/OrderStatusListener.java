package com.ecommerce.order.status;

import com.ecommerce.common.event.OrderPaidEvent;
import com.ecommerce.common.event.OrderShippedEvent;
import com.ecommerce.common.event.StockReservedEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Listens to every downstream topic purely to keep the dashboard's status board up to date. */
@Component
public class OrderStatusListener {

    private final OrderStatusStore store;

    public OrderStatusListener(OrderStatusStore store) {
        this.store = store;
    }

    @KafkaListener(topics = "stock.reserved", groupId = "order-status-group",
            containerFactory = "stockReservedListenerFactory")
    public void onStockReserved(StockReservedEvent event) {
        store.stockReserved(event);
    }

    @KafkaListener(topics = "order.paid", groupId = "order-status-group",
            containerFactory = "orderPaidListenerFactory")
    public void onOrderPaid(OrderPaidEvent event) {
        store.orderPaid(event);
    }

    @KafkaListener(topics = "order.shipped", groupId = "order-status-group",
            containerFactory = "orderShippedListenerFactory")
    public void onOrderShipped(OrderShippedEvent event) {
        store.orderShipped(event);
    }
}

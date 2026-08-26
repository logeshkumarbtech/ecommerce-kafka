package com.ecommerce.inventory.listener;

import com.ecommerce.common.event.OrderCreatedEvent;
import com.ecommerce.common.event.StockReservedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class InventoryEventListener {

    private static final Logger log = LoggerFactory.getLogger(InventoryEventListener.class);
    private static final String OUT_TOPIC = "stock.reserved";

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public InventoryEventListener(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = "order.created", groupId = "inventory-group")
    public void onOrderCreated(OrderCreatedEvent event) {
        log.info("[INVENTORY] Received order.created for orderId={} productId={} qty={}",
                event.orderId(), event.productId(), event.quantity());

        // Simulate stock reservation
        log.info("[INVENTORY] Reserving {} units of product {}", event.quantity(), event.productId());

        StockReservedEvent stockReserved = new StockReservedEvent(
                event.orderId(),
                event.productId(),
                event.quantity()
        );

        kafkaTemplate.send(OUT_TOPIC, event.orderId(), stockReserved);
        log.info("[INVENTORY] Published stock.reserved for orderId={}", event.orderId());
    }
}

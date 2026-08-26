package com.ecommerce.payment.listener;

import com.ecommerce.common.event.OrderPaidEvent;
import com.ecommerce.common.event.StockReservedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class PaymentEventListener {

    private static final Logger log = LoggerFactory.getLogger(PaymentEventListener.class);
    private static final String OUT_TOPIC = "order.paid";

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public PaymentEventListener(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = "stock.reserved", groupId = "payment-group")
    public void onStockReserved(StockReservedEvent event) {
        log.info("[PAYMENT] Received stock.reserved for orderId={}", event.orderId());

        // Simulate payment charge
        String transactionId = UUID.randomUUID().toString();
        log.info("[PAYMENT] Charging customer — transactionId={}", transactionId);

        // customerId not available here; notification-service will pick it from order.paid via orderId lookup
        OrderPaidEvent paidEvent = new OrderPaidEvent(
                event.orderId(),
                "customer-resolved-by-order-service",
                0.0,
                transactionId
        );

        kafkaTemplate.send(OUT_TOPIC, event.orderId(), paidEvent);
        log.info("[PAYMENT] Published order.paid for orderId={}", event.orderId());
    }
}

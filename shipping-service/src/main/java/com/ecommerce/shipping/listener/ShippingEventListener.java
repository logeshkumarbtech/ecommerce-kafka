package com.ecommerce.shipping.listener;

import com.ecommerce.common.event.OrderPaidEvent;
import com.ecommerce.common.event.OrderShippedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.util.UUID;

@Component
public class ShippingEventListener {

    private static final Logger log = LoggerFactory.getLogger(ShippingEventListener.class);
    private static final String OUT_TOPIC = "order.shipped";

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public ShippingEventListener(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = "order.paid", groupId = "shipping-group")
    public void onOrderPaid(OrderPaidEvent event) {
        log.info("[SHIPPING] Received order.paid for orderId={}", event.orderId());

        String trackingNumber = "TRK-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        String estimatedDelivery = LocalDate.now().plusDays(3).toString();

        log.info("[SHIPPING] Dispatching order — tracking={} ETA={}", trackingNumber, estimatedDelivery);

        OrderShippedEvent shippedEvent = new OrderShippedEvent(
                event.orderId(),
                event.customerId(),
                trackingNumber,
                estimatedDelivery
        );

        kafkaTemplate.send(OUT_TOPIC, event.orderId(), shippedEvent);
        log.info("[SHIPPING] Published order.shipped for orderId={}", event.orderId());
    }
}

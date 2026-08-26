package com.ecommerce.notification.listener;

import com.ecommerce.common.event.OrderPaidEvent;
import com.ecommerce.common.event.OrderShippedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class NotificationEventListener {

    private static final Logger log = LoggerFactory.getLogger(NotificationEventListener.class);

    @KafkaListener(topics = "order.paid", groupId = "notification-group",
            containerFactory = "paidListenerFactory")
    public void onOrderPaid(OrderPaidEvent event) {
        log.info("[NOTIFICATION] Order paid — sending payment confirmation email");
        log.info("[NOTIFICATION] → orderId={} transactionId={}",
                event.orderId(), event.transactionId());
        // Simulated: emailService.sendPaymentConfirmation(event.customerId(), event.orderId())
    }

    @KafkaListener(topics = "order.shipped", groupId = "notification-group",
            containerFactory = "shippedListenerFactory")
    public void onOrderShipped(OrderShippedEvent event) {
        log.info("[NOTIFICATION] Order shipped — sending shipping update SMS/email");
        log.info("[NOTIFICATION] → orderId={} tracking={} ETA={}",
                event.orderId(), event.trackingNumber(), event.estimatedDelivery());
        // Simulated: smsService.sendTrackingUpdate(event.customerId(), event.trackingNumber())
    }
}

package com.ecommerce.order.config;

import com.ecommerce.common.event.OrderPaidEvent;
import com.ecommerce.common.event.OrderShippedEvent;
import com.ecommerce.common.event.StockReservedEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.Map;

/** Consumer factories used only by the dashboard's status listener. */
@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    private Map<String, Object> baseConsumerProps() {
        return Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, "order-status-group",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
        );
    }

    @Bean
    public ConsumerFactory<String, StockReservedEvent> stockReservedConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(baseConsumerProps(),
                new StringDeserializer(), new JsonDeserializer<>(StockReservedEvent.class, false));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, StockReservedEvent> stockReservedListenerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, StockReservedEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(stockReservedConsumerFactory());
        return factory;
    }

    @Bean
    public ConsumerFactory<String, OrderPaidEvent> orderPaidConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(baseConsumerProps(),
                new StringDeserializer(), new JsonDeserializer<>(OrderPaidEvent.class, false));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderPaidEvent> orderPaidListenerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, OrderPaidEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(orderPaidConsumerFactory());
        return factory;
    }

    @Bean
    public ConsumerFactory<String, OrderShippedEvent> orderShippedConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(baseConsumerProps(),
                new StringDeserializer(), new JsonDeserializer<>(OrderShippedEvent.class, false));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderShippedEvent> orderShippedListenerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, OrderShippedEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(orderShippedConsumerFactory());
        return factory;
    }
}

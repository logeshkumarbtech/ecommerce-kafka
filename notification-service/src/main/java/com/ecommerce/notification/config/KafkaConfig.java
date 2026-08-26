package com.ecommerce.notification.config;

import com.ecommerce.common.event.OrderPaidEvent;
import com.ecommerce.common.event.OrderShippedEvent;
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

@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    private Map<String, Object> baseConsumerProps() {
        return Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, "notification-group",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
        );
    }

    @Bean
    public ConsumerFactory<String, OrderPaidEvent> paidConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(baseConsumerProps(),
                new StringDeserializer(),
                new JsonDeserializer<>(OrderPaidEvent.class, false));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderPaidEvent> paidListenerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, OrderPaidEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(paidConsumerFactory());
        return factory;
    }

    @Bean
    public ConsumerFactory<String, OrderShippedEvent> shippedConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(baseConsumerProps(),
                new StringDeserializer(),
                new JsonDeserializer<>(OrderShippedEvent.class, false));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderShippedEvent> shippedListenerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, OrderShippedEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(shippedConsumerFactory());
        return factory;
    }
}

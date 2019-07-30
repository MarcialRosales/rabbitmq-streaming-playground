package com.example.demo;

import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.scheduler.Schedulers;
import reactor.rabbitmq.*;

@Configuration
public class RabbitMQConfiguration {

    @Autowired
    org.springframework.amqp.rabbit.connection.ConnectionFactory springConnectionFactory;

    @Autowired
    com.rabbitmq.client.ConnectionFactory connectionFactory;

    public ReceiverOptions receiverOptions() {
        return withReceiverOptions(connectionFactory);
    }

    @Bean
    com.rabbitmq.client.ConnectionFactory connectionFactory() {
        CachingConnectionFactory ccf = (CachingConnectionFactory) springConnectionFactory.getPublisherConnectionFactory();
        return ccf.getRabbitConnectionFactory();
    }

    public Sender createSender() {
        return RabbitFlux.createSender(withSenderOptions(connectionFactory));
    }
    public Receiver createReceiver() {
        return RabbitFlux.createReceiver(withReceiverOptions(connectionFactory));
    }

    private ReceiverOptions withReceiverOptions(com.rabbitmq.client.ConnectionFactory connectionFactory) {
        return new ReceiverOptions()
                .connectionFactory(connectionFactory)
                .connectionSubscriptionScheduler(Schedulers.elastic());
    }

    private SenderOptions withSenderOptions(com.rabbitmq.client.ConnectionFactory connectionFactory) {
        SenderOptions senderOptions = new SenderOptions()
                .connectionFactory(connectionFactory)
                .resourceManagementScheduler(Schedulers.elastic());
        return senderOptions;
    }

}
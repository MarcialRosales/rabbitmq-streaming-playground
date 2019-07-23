package com.example.demo;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.amqp.RabbitProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Mono;
import reactor.rabbitmq.*;
import reactor.retry.Retry;

import javax.annotation.PreDestroy;
import java.time.Duration;

@Configuration
public class RabbitMQConfiguration {

    @Bean
    Retry retryPolicy() {
        return Retry.any()
                .randomBackoff(Duration.ofMillis(100), Duration.ofSeconds(2))
                .doOnRetry(context -> {
                    System.out.printf("Failed attempt %d due to [%s]\n",
                            context.iteration(),
                            context.exception().getMessage(),
                            context.backoff().toMillis(),
                            context.applicationContext());
                })
                .timeout(Duration.ofSeconds(60))
                .retryMax(10);
    }

    @Bean()
    Mono<Connection> connection(RabbitProperties rabbitProperties, Retry retryPolicy) {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost(rabbitProperties.getHost());
        connectionFactory.setPort(rabbitProperties.getPort());
        connectionFactory.setUsername(rabbitProperties.getUsername());
        connectionFactory.setPassword(rabbitProperties.getPassword());

        return Mono.fromCallable(() -> connectionFactory.newConnection())
                .retryWhen(retryPolicy)
                .doOnNext(v -> System.out.printf("Finally Received %s\n ", v))
                .cache();
    }

    @Bean
    Sender sender(Mono<Connection> connectionMono) {
        return RabbitFlux.createSender(new SenderOptions().connectionMono(connectionMono));
    }

    @Bean
    Receiver receiver(Mono<Connection> connectionMono) {
        return RabbitFlux.createReceiver(new ReceiverOptions().connectionMono(connectionMono));
    }

    @Autowired
    private Mono<Connection> connection;

    @PreDestroy
    public void close() throws Exception {
        connection.block().close();
    }

}

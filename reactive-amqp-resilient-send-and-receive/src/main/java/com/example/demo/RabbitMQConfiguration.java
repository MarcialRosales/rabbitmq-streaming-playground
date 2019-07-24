package com.example.demo;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.amqp.RabbitProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import reactor.core.publisher.Mono;
import reactor.rabbitmq.*;
import reactor.retry.Retry;

import javax.annotation.PreDestroy;
import java.time.Duration;

@Configuration
@EnableConfigurationProperties({ConnectionRetryPolicyProperties.class})
public class RabbitMQConfiguration {

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMQConfiguration.class);

    @Bean
    Retry retryPolicy(ConnectionRetryPolicyProperties retryProperties) {
        return Retry.any()
                .randomBackoff(retryProperties.randomBackoff.firstBackoff, retryProperties.randomBackoff.maxBackoff)
                .doOnRetry(context -> {
                    LOGGER.error("Failed attempt {} due to {} [backoff:{}ms]",
                            context.iteration(),
                            context.exception().getMessage(),
                            context.backoff().toMillis());
                })
                .timeout(retryProperties.timeout)
                .retryMax(retryProperties.retryMax);
    }

    @Bean
    ConnectionFactory connectionFactory(RabbitProperties rabbitProperties) {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost(rabbitProperties.getHost());
        connectionFactory.setPort(rabbitProperties.getPort());
        connectionFactory.setUsername(rabbitProperties.getUsername());
        connectionFactory.setPassword(rabbitProperties.getPassword());
        connectionFactory.useNio();

        return connectionFactory;
    }
    @Bean("senderConnection")
    Mono<Connection> senderConnection(ConnectionFactory connectionFactory, Retry retryPolicy) {
        return Mono.fromCallable(() -> connectionFactory.newConnection("sender"))
                .retryWhen(retryPolicy)
                .doOnNext(v -> LOGGER.info("Sender Connection established with {}", v))
                .cache();
    }

    @Bean("receiverConnection")
    Mono<Connection> receiverConnection(ConnectionFactory connectionFactory, Retry retryPolicy) {
        return Mono.fromCallable(() -> connectionFactory.newConnection("receiver"))
                .retryWhen(retryPolicy)
                .doOnNext(v -> LOGGER.info("Receiver Connection established with {}", v))
                .cache();
    }

    @Bean
    Sender sender(@Qualifier("senderConnection") Mono<Connection> connectionMono) {
        return RabbitFlux.createSender(new SenderOptions().connectionMono(connectionMono));
    }

    @Bean
    Receiver receiver(@Qualifier("receiverConnection") Mono<Connection> connectionMono) {
        return RabbitFlux.createReceiver(new ReceiverOptions().connectionMono(connectionMono));
    }

    @Autowired
    private Mono<Connection> senderConnection;
    @Autowired
    private Mono<Connection> receiverConnection;

    @PreDestroy
    public void closeSender() throws Exception {
        senderConnection.block().close();
    }
    @PreDestroy
    public void closeReceiver() throws Exception {
        receiverConnection.block().close();
    }

}
@ConfigurationProperties(prefix = "spring.rabbitmq.connection")
class ConnectionRetryPolicyProperties {

    long retryMax = 10;
    Duration timeout = Duration.ofMillis(60000);

    RandomBackoffProperties randomBackoff = new RandomBackoffProperties();

    public long getRetryMax() {
        return retryMax;
    }

    public void setRetryMax(long retryMax) {
        this.retryMax = retryMax;
    }

    public long getTimeoutMs() {
        return timeout.toMillis();
    }

    public void setTimeoutMs(long timeoutMs) {
        this.timeout = Duration.ofMillis(timeoutMs);
    }


}
class RandomBackoffProperties {
    Duration firstBackoff = Duration.ofMillis(1000);
    Duration maxBackoff = Duration.ofMillis(10000);;

    public long getFirstBackoffMs() {
        return firstBackoff.toMillis();
    }

    public void setFirstBackoffMs(long firstBackoffMs) {
        this.firstBackoff = Duration.ofMillis(firstBackoffMs);
    }

    public long getMaxBackoffMs() {
        return maxBackoff.toMillis();
    }

    public void setMaxBackoff(long maxBackoffMs) {
        this.maxBackoff = Duration.ofMillis(maxBackoffMs);
    }
}

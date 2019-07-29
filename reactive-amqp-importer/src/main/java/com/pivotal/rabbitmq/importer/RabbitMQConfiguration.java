package com.pivotal.rabbitmq.importer;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.amqp.RabbitProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
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
                    LOGGER.error("Failed attempt %d due to [%s]\n",
                            context.iteration(),
                            context.exception().getMessage(),
                            context.backoff().toMillis(),
                            context.applicationContext());
                })
                .timeout(retryProperties.timeout)
                .retryMax(retryProperties.retryMax);
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
                .doOnNext(v -> LOGGER.info("Connection established with {}", v))
                .cache();
    }


    Sender sender() {
        return RabbitFlux.createSender(new SenderOptions().connectionMono(connection));
    }

    @Autowired
    private Mono<Connection> connection;

    @PreDestroy
    public void close() throws Exception {
        // This is required because sender.close() does not close the connection if it is provided via a Mono
        // only when we don't provide a Mono. Maybe it should be specified in the docs.
        connection.block().close();
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

package com.example.demo;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Recoverable;
import com.rabbitmq.client.RecoveryListener;
import com.rabbitmq.http.client.Client;
import com.rabbitmq.http.client.ReactorNettyClient;
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
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.time.Duration;

/**
 * CONSIDERATIONS:
 *
 * 1) nio
 *      With the default blocking IO mode, each connection uses a thread to read from the network socket.
 *      With the NIO mode, you can control the number of threads that read and write from/to the network socket.
 *
 *      Use the NIO mode if your Java process uses many connections (dozens or hundreds). You should use fewer threads
 *      than with the default blocking mode.
 *
 *      More https://www.rabbitmq.com/api-guide.html#java-nio
 *
 * 2) MessageDelivery thread
 *
 *      When we use Nio, a default number of nio threads are created to attend network i/o.
 *      All callbacks from the broker, such as Confirms and Returns, are delivered via `rabbitmq-nio` thread.
 *      This is the reason by the "publisher" thread for the OutboundMessageResult is not the same thread that
 *      originally generated the message (by default it is the resource declaration scheduler).
 *
 */
@Configuration
@EnableConfigurationProperties({ConnectionRetryPolicyProperties.class})
public class RabbitMQConfiguration {

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMQConfiguration.class);

    @Bean
    Retry<?> retryPolicy(ConnectionRetryPolicyProperties retryProperties) {
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
        connectionFactory.useNio();     // is this really necessary? I think it is automatically enabled
        
        return connectionFactory;
    }
    @Bean
    Mono<Connection> senderConnection(ConnectionFactory connectionFactory, Retry<?> retryPolicy) {
        return buildNamedConnection(connectionFactory, retryPolicy, "sender");
    }

    @Bean
    Mono<Connection> receiverConnection(ConnectionFactory connectionFactory, Retry<?> retryPolicy) {
        return buildNamedConnection(connectionFactory, retryPolicy, "receiver");
    }

    private Mono<Connection> buildNamedConnection(ConnectionFactory connectionFactory, Retry<?> retryPolicy, String name) {
        return Mono.fromCallable(() -> connectionFactory.newConnection(name)).log()
                .retryWhen(retryPolicy)
                .doOnNext(conn -> LOGGER.info("{} Connection established with {}", name, conn))
                .cache(); // when we create the Mono<Connection>, RabbitMQ Reactive client does not automatically cache  it hence we do it.
    }

    @Bean
    ReactorNettyClient hop(RabbitProperties properties) {
        return new ReactorNettyClient("http://localhost:15672/api/", "guest", "guest");
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
        // when we create the Mono<Connection>, RabbitMQ Reactive client does not automatically close the connection
        // when we close Sender/Receiver. We have to do it.
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

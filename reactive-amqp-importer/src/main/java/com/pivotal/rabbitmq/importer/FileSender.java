package com.pivotal.rabbitmq.importer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.event.EventListener;
import org.springframework.retry.ExhaustedRetryException;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.rabbitmq.ExchangeSpecification;
import reactor.rabbitmq.OutboundMessage;
import reactor.rabbitmq.QueueSpecification;
import reactor.rabbitmq.Sender;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.function.Function;
import java.util.stream.BaseStream;

@Service
@EnableConfigurationProperties(FileSenderConfigurationProperties.class)
public class FileSender {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileSender.class);

    @Autowired
    private Sender sender;

    @Autowired
    private FileSenderConfigurationProperties properties;

    @EventListener
    public void sendFile(ApplicationStartedEvent event) {
        Mono<?> untilQueueExists = sender.declareQueue(queue());

        sender.send(messagesFromFile())
              .delaySubscription(untilQueueExists)
              .block();
    }
    Flux<OutboundMessage> messagesFromFile() {
        return readLinesFromFile(properties.filename)
                .map(toMessage(exchange(), properties.routingKey))
                .delayElements(Duration.ofMillis(100))
                .doOnComplete(() -> LOGGER.info("sent all lines"));
    }
    private Function<String, OutboundMessage> toMessage(ExchangeSpecification exchange, String routingKey) {
        return (String v) -> new OutboundMessage(exchange.getName(), routingKey, v.getBytes());
    }
    private Flux<String> readLinesFromFile(String filename) {
        return fromPath(Paths.get(filename)).filter(s -> !s.startsWith("#"));
    }
    private Flux<String> fromPath(Path path) {
        return Flux.using(() -> Files.lines(path),
                Flux::fromStream,
                BaseStream::close
        ).doOnCancel(() -> LOGGER.warn("cancelled"));
    }

    private QueueSpecification queue() {
        return QueueSpecification.queue(properties.queue).durable(properties.durableQueue);
    }
    private ExchangeSpecification exchange() {
        return ExchangeSpecification.exchange(properties.exchange);
    }

}

@ConfigurationProperties("file-sender")
class FileSenderConfigurationProperties {
    String queue;
    boolean durableQueue;
    String exchange = "";
    String routingKey = "";
    String filename;

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public boolean isDurableQueue() {
        return durableQueue;
    }

    public void setDurableQueue(boolean durableQueue) {
        this.durableQueue = durableQueue;
    }

    public String getExchange() {
        return exchange;
    }

    public void setExchange(String exchange) {
        this.exchange = exchange;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    public void setRoutingKey(String routingKey) {
        this.routingKey = routingKey;
    }

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }
}
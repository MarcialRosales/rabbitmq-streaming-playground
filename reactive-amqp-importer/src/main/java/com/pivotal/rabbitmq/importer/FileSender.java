package com.pivotal.rabbitmq.importer;

import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.amqp.RabbitAutoConfiguration;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.rabbitmq.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.BaseStream;

@Service
@EnableConfigurationProperties(FileSenderConfigurationProperties.class)
public class FileSender {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileSender.class);

    @Autowired
    private FileSenderConfigurationProperties properties;

    @Autowired
    RabbitMQConfiguration rabbit;


    @EventListener
    public void sendFile(ApplicationStartedEvent event) throws InterruptedException {
        LOGGER.info("FileSender using {}", properties);

        CountDownLatch terminated = new CountDownLatch(1);

        Sender sender = rabbit.sender();
        ResourceDeclaration requiredResources = new ResourceDeclaration(sender, properties);

        sender
                .sendWithPublishConfirms(andCount(messagesFromFile(Paths.get(properties.filename)), readCount))
                .delaySubscription(requiredResources.declare())
                .doOnNext(this::count)
                .doFinally(this::printSummary)
                .doOnTerminate(terminated::countDown)
                .subscribe();

        terminated.await();
        sender.close();

    }

    void printSummary(SignalType signal) {
            LOGGER.info("Summary ({}): read {} lines. {} sent / {} nacked / {} returned",
                    signal,
                    readCount.get(), publishedMessageCount.get(), nackedMessageCount.get(),
                    returnedMessageCount.get());
    }


    AtomicLong readCount = new AtomicLong();
    AtomicLong returnedMessageCount = new AtomicLong();
    AtomicLong nackedMessageCount = new AtomicLong();
    AtomicLong publishedMessageCount = new AtomicLong();

    private void count(OutboundMessageResult result) {
        if (result.isReturned()) {
            returnedMessageCount.incrementAndGet();
        }else {
            if (result.isAck()) {
                publishedMessageCount.incrementAndGet();
            }else {
                nackedMessageCount.incrementAndGet();
            }
        }
    }

    Flux<OutboundMessage> andCount(Flux<OutboundMessage> messages, AtomicLong counter) {
        return messages.doOnNext(l -> counter.incrementAndGet());
    }

    private Flux<OutboundMessage> messagesFromFile(Path path) {
        return FromFile.lines(path)
                .filter(properties::shouldSkipLine)
                .map(l -> new OutboundMessage(properties.exchange, properties.getRoutingKey(), l.getBytes()));
    }

//    void sendWithoutDeliveryGuaranteedUsing(Sender sender, ResourceDeclaration requiredResources) {
//        requiredResources.declare()
//                .thenMany(sender.send(andCount(messagesFromFile(), readCount), new SendOptions()))
//                .doOnError(this::log)
//                .doOnComplete(() -> LOGGER.info("{} published ",
//                        publishedMessageCount.get()))
//                .subscribe();
//
//    }
}


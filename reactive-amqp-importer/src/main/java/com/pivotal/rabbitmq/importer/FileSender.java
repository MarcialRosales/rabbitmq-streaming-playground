package com.pivotal.rabbitmq.importer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.SignalType;
import reactor.rabbitmq.OutboundMessage;
import reactor.rabbitmq.OutboundMessageResult;
import reactor.rabbitmq.Sender;

import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

@Service
@EnableConfigurationProperties(FileSenderConfigurationProperties.class)
public class FileSender {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileSender.class);

    @Autowired
    private FileSenderConfigurationProperties properties;

    @Autowired
    RabbitMQConfiguration rabbit;


    // Useful snippets
/*
    private void printOutLinesFromWeb() {
        CountDownLatch terminated = new CountDownLatch(1);
        MessageSource.fromWeb(properties.getUri()).index().doOnTerminate(terminated::countDown).subscribe(System.out::println);
        terminated.await();
    }
*/

    @EventListener
    public void sendFile(ApplicationStartedEvent event) throws InterruptedException {
        LOGGER.info("FileSender using {}", properties);

        CountDownLatch terminated = new CountDownLatch(1);

        Sender sender = rabbit.sender();
        ResourceDeclaration requiredResources = new ResourceDeclaration(sender, properties);

        sender
                .sendWithPublishConfirms(andCount(messagesFromFile(properties.getUri()), readCount))
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


    private AtomicLong readCount = new AtomicLong();
    private AtomicLong returnedMessageCount = new AtomicLong();
    private AtomicLong nackedMessageCount = new AtomicLong();
    private AtomicLong publishedMessageCount = new AtomicLong();

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

    private Flux<OutboundMessage> messagesFromFile(URI uri) {
        return MessageSource.lines(uri)
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


package com.pivotal.rabbitmq.exporter;

import com.rabbitmq.client.Delivery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.rabbitmq.ConsumeOptions;
import reactor.rabbitmq.Receiver;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Service
@EnableConfigurationProperties(QueueExporterConfigurationProperties.class)
public class QueueExporter {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueueExporter.class);

    @Autowired
    private QueueExporterConfigurationProperties properties;

    @Autowired
    RabbitMQConfiguration rabbit;


    @EventListener
    public void exportQueue(ApplicationStartedEvent event) throws InterruptedException, IOException {
        LOGGER.debug("Using the following settings \n\t{}", properties);

        CountDownLatch terminated = new CountDownLatch(1);

        Receiver receiver = rabbit.receiver();
        optionallyTerminateIfIdle(receiver.consumeNoAck(properties.queue, new ConsumeOptions()))

                .doOnNext(d -> receivedMessageCount.incrementAndGet())
                .doFinally(this::printSummary)
                .subscribe(d -> LOGGER.info("{}", new String(d.getBody())),
                        e -> LOGGER.error("An error occurred", e),
                        terminated::countDown);

        terminated.await();

        receiver.close();

    }
    Flux<Delivery> optionallyTerminateIfIdle(Flux<Delivery> fluxOfMessages) {
        if (properties.idleTimeSec > -1) {
            return fluxOfMessages.timeout(Duration.ofSeconds(properties.idleTimeSec), Mono.empty());
        }else {
            return fluxOfMessages;
        }
    }
    void printSummary(SignalType signal) {
            LOGGER.info("Exported {} messages to console  - Completed due to {}",
                    receivedMessageCount.get(),
                    signal);
    }

    private AtomicLong receivedMessageCount = new AtomicLong();


}



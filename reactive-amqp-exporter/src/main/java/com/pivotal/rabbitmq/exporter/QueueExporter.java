package com.pivotal.rabbitmq.exporter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;
import reactor.core.publisher.SignalType;
import reactor.rabbitmq.ConsumeOptions;
import reactor.rabbitmq.Receiver;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import static com.pivotal.rabbitmq.exporter.MessageSink.sendToFileInBatches;

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

        Path file = Files.createTempFile(String.format("%s-", properties.queue), null);
        LOGGER.info("Exporting messages to {}", file);

        CountDownLatch terminated = new CountDownLatch(1);

        Receiver receiver = rabbit.receiver();
        receiver.consumeManualAck(properties.queue, new ConsumeOptions().qos(properties.getQos()))
                .transform(sendToFileInBatches(file, properties.batchSize, Duration.ofMillis(properties.getConsumeTimeout())))
                .doOnNext(this::count)
                .doOnNext(batch -> terminateIfIdle(batch, terminated))
                .doFinally(s -> printSummary(s, file))
                .subscribe();

        terminated.await();
        receiver.close();

    }
    void count(AcknowledgableDeliveryBatch batch) {
        if (!batch.isEmpty()) receivedMessageCount.addAndGet(batch.size());
    }
    void terminateIfIdle(AcknowledgableDeliveryBatch batch, CountDownLatch terminated) {
        if (batch.isEmpty()) terminated.countDown();
    }

    void printSummary(SignalType signal, Path file) {
            LOGGER.info("Exported {} messages to {}  - Completed due to {}",
                    receivedMessageCount.get(),
                    file,
                    signal);
    }

    private AtomicLong receivedMessageCount = new AtomicLong();


}



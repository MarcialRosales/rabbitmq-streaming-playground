package com.pivotal.rabbitmq.importer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.event.EventListener;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.rabbitmq.*;
import sun.plugin2.message.Message;

import java.io.IOException;
import java.net.URI;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CountDownLatch;
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
        LOGGER.info("QueueExporter using {}", properties);

        Path file = Files.createTempFile(properties.queue, null);
        LOGGER.info("Exporting messages to {}", file);

        WritableByteChannel channel = Files.newByteChannel(file, StandardOpenOption.WRITE);

        CountDownLatch terminated = new CountDownLatch(1);

        Receiver receiver = rabbit.receiver();
        receiver
                .consumeManualAck(properties.queue, new ConsumeOptions())
                .doOnNext(d -> receivedMessageCount.incrementAndGet())
                .concatMap(d -> MessageSink.write(d, channel).doOnSuccess(v -> {
                    d.ack();
                    ackedMessageCount.incrementAndGet();
                }))
                .doFinally((s)-> {
                    try {
                        channel.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    printSummary(s);
                })
                .doOnTerminate(terminated::countDown)
                .subscribe();


//        MessageSink.toFile(messages, properties.queue)
//                .doFinally(this::printSummary)
//                .doOnTerminate(terminated::countDown)
//                .subscribe(System.out::println);

        terminated.await();
        receiver.close();

    }

    void printSummary(SignalType signal) {
            LOGGER.info("Summary ({}): {} received / {} acked",
                    signal,
                    receivedMessageCount.get(),
                    ackedMessageCount.get());
    }

    private AtomicLong receivedMessageCount = new AtomicLong();
    private AtomicLong ackedMessageCount = new AtomicLong();

    private void count(AcknowledgableDelivery result) {
        receivedMessageCount.incrementAndGet();
    }

}


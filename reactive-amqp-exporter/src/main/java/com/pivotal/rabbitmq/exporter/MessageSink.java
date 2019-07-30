package com.pivotal.rabbitmq.exporter;

import com.rabbitmq.client.Delivery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.rabbitmq.AcknowledgableDelivery;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

public class MessageSink {
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageSink.class);

    public static Function<Flux<AcknowledgableDelivery>, Flux<AcknowledgableDeliveryBatch>>
    sendToFileInBatches(Path file, int batchSize, Duration batchTimeout) throws IOException {

        FileChannel channel = FileChannel.open(file, StandardOpenOption.WRITE);
        AcknowledgableDeliveryBatchRecorder lastMessage = new AcknowledgableDeliveryBatchRecorder();
        DefaultDataBufferFactory dataBufferFactory = new DefaultDataBufferFactory();

        Function<Flux<AcknowledgableDelivery>, Flux<DataBuffer>> toDataBuffer = f -> f
                .doOnNext(lastMessage::cache)
                .map(Delivery::getBody)
                .map(dataBufferFactory::<DataBuffer>wrap);

        Function<Flux<AcknowledgableDelivery>, Flux<AcknowledgableDeliveryBatch>>
                sendToFile =
                f -> f.windowTimeout(batchSize, batchTimeout)
                       .concatMap(messages ->
                        DataBufferUtils
                                .write(messages.transform(toDataBuffer), channel)
                                .then(lastMessage.toMono())
                )
                        .doOnNext(flushAndAck(channel))
                        .doFinally(close(channel));

        return sendToFile;
    }

    private static Consumer<SignalType> close(FileChannel fileChannel) {
        return s -> {
            try {
                fileChannel.close();
                LOGGER.debug("Closed file due to %s", s.name());
            } catch (IOException e) {
                LOGGER.error("Failed to close disk", e);
                throw Exceptions.propagate(e);
            }
        };
    }
    private static Consumer<AcknowledgableDeliveryBatch> flushAndAck(FileChannel fileChannel) {
        return tuple -> {
            if (tuple.isEmpty()) {
                return;
            }

            AcknowledgableDelivery first = tuple.first();
            AcknowledgableDelivery last = tuple.last();
            try {
                fileChannel.force(false);
                LOGGER.debug("Ack from %d to %d", first.getEnvelope().getDeliveryTag(),
                        last.getEnvelope().getDeliveryTag());
                last.ack(true);
            } catch (IOException e) {
                LOGGER.error("Failed to flush to disk", e);
                last.nack(true, true);
                LOGGER.debug("Nack from %d to %d", first.getEnvelope().getDeliveryTag(),
                        last.getEnvelope().getDeliveryTag());
                throw Exceptions.propagate(e);
            }
        };
    }
}
class AcknowledgableDeliveryBatch {
    private AcknowledgableDelivery first;
    private AcknowledgableDelivery last;
    private int size;

    public AcknowledgableDeliveryBatch(AcknowledgableDelivery first, AcknowledgableDelivery last, int count) {
        this.first = first;
        this.last = last;
        this.size = count;
    }

    public AcknowledgableDelivery first() {
        return first;
    }

    public AcknowledgableDelivery last() {
        return last;
    }
    public int size() {
        return size;
    }
    public boolean isEmpty() {
        return first == null;
    }
}
class AcknowledgableDeliveryBatchRecorder {
    private AtomicReference<AcknowledgableDelivery> first = new AtomicReference<>();
    private AtomicReference<AcknowledgableDelivery> last = new AtomicReference<>();
    private AtomicInteger count = new AtomicInteger();

    public void cache(AcknowledgableDelivery value) {
        first.compareAndSet(null, value);
        last.set(value);
        count.incrementAndGet();
    }

    private Mono<AcknowledgableDeliveryBatch> mono = Mono.fromSupplier(
            () -> new AcknowledgableDeliveryBatch(first.getAndSet(null),
                    last.getAndSet(null), count.getAndSet(0)));

    public Mono<AcknowledgableDeliveryBatch> toMono() {
        return mono;
    }
}

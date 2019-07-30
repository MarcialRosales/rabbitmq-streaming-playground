package com.pivotal.rabbitmq.exporter;


import org.junit.Test;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

public class QueueExporterTest {

    @Test
    public void nullOrEmptyMonoEmitsCompleteSignal() {
        Mono<Integer> mono = Mono.fromSupplier(() -> null);
        mono.subscribe(
                v -> System.out.println("Got a value"),
                e -> System.err.println(e),
                () -> System.out.println("finished"));
    }

    @Test
    public void writeToFile() throws IOException, InterruptedException {
        Path file = Files.createTempFile("test", null);
        System.out.println(file);
        DefaultDataBufferFactory dataBufferFactory = new DefaultDataBufferFactory();

        FileChannel fileChannel = FileChannel.open(file, StandardOpenOption.WRITE);

        AtomicReference<Integer> lastInteger = new AtomicReference<>();
        Mono<Integer> returnLastInteger = Mono.fromSupplier(lastInteger::get);

        Function<Flux<Integer>, Flux<DataBuffer>> toDataBuffer = f -> f
                .doOnNext(lastInteger::set)
                .map(i -> String.valueOf(i + ",").getBytes())
                .doOnSubscribe(s -> System.out.printf("["))
                .doOnNext(v -> System.out.printf("%s", new String(v)))
                .map(dataBufferFactory::<DataBuffer>wrap);

        Flux.range(0, 105)
                .windowTimeout(10, Duration.ofMillis(250))
                .concatMap(integers ->
                        DataBufferUtils
                                .write(integers.transform(toDataBuffer), fileChannel)
                                .then(returnLastInteger)
                )
                .doOnNext(this::flushAndAck)
                .doFinally(close(fileChannel))
                .subscribe();
    }
    Consumer<SignalType> close(FileChannel fileChannel) {
        return s -> {
            try {
                System.out.printf("Finally due to %s. Closing file", s.name());
                fileChannel.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        };
    }
    Random rand = new Random(System.currentTimeMillis());

    private void flushAndAck(Integer lastSeenInteger) {
        try {
            flush(rand);
            System.out.printf("]# ack %d\n", lastSeenInteger);
        }catch(Throwable t) {
            System.out.printf("]# nack %d due to %s\n", lastSeenInteger, t.getMessage());
            throw Exceptions.propagate(t);
        }
    }
    private void flush(Random rand) {
       System.out.printf(" flushing ");
        if (rand.nextBoolean()) {
           // throw Exceptions.propagate(new IOException("simulated"));
        }
    }
    /*
    return DataBufferUtils.write(w

                                    .map(i -> String.valueOf(i + ",").getBytes())
                                    .doOnSubscribe(s -> System.out.printf("["))
                                    .doOnNext(v -> System.out.printf("%s", new String(v)))
                                    .map(b -> dataBufferFactory.<DataBuffer>wrap(b))
                            , fileChannel).doOnComplete(() -> {
                        try {
                            fileChannel.force(false);
                            System.out.println(" flushing");
                        } catch (IOException e) {
                            throw Exceptions.propagate(e);
                        }
                    });
     */
}
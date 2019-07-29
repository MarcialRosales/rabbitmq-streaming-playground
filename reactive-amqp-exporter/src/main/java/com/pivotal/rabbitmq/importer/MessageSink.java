package com.pivotal.rabbitmq.importer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.rabbitmq.AcknowledgableDelivery;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class MessageSink {
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageSink.class);


    private static DefaultDataBufferFactory dataBufferFactory = new DefaultDataBufferFactory();

    public static Mono<Path> toFile(Flux<AcknowledgableDelivery> messages, String withPrefix) throws IOException {

        Path file = Files.createTempFile(withPrefix, null);

        Flux<DataBuffer> dataBuffer = messages.map(
                m -> dataBufferFactory.<DataBuffer>wrap(m.getBody()));

        WritableByteChannel channel = Files.newByteChannel(file, StandardOpenOption.WRITE);
        return DataBufferUtils
                .write(dataBuffer, channel)
                .map(DataBufferUtils::release)
                .then(Mono.just(file));

    }

    public static Mono<Void> write(AcknowledgableDelivery message, WritableByteChannel channel ) {
        return DataBufferUtils
                .write(Flux.just(dataBufferFactory.wrap(message.getBody())), channel)
                .map(DataBufferUtils::release).then();
    }
}

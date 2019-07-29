package com.pivotal.rabbitmq.importer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.netty.http.client.HttpClient;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.BaseStream;

public class FromFile {
    private static final Logger LOGGER = LoggerFactory.getLogger(FromFile.class);

    public static Flux<String> lines(Path path) {
        return Flux.using(() -> Files.lines(path),  // 1) generate the disposable resource
                Flux::fromStream,                   // 2) create a Flux<String> from a disposable resource from 1)
                BaseStream::close                   // 3) when Flux completes, we close the disposable resource
        ).doOnCancel(() -> LOGGER.warn("cancelled"));
    }

}

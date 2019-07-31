package com.pivotal.rabbitmq.importer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.netty.http.client.HttpClient;

import javax.annotation.PreDestroy;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Scanner;
import java.util.function.Consumer;
import java.util.stream.BaseStream;

public class MessageSource {
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageSource.class);

    public static Flux<String> lines(URI uri) {
        switch(uri.getScheme() == null ? "file" : uri.getScheme()) {
            case "file":
                return fromFile(Paths.get(uri.toASCIIString()));
            case "http":
            case "https":
                return fromWeb(uri);

        }
        throw new IllegalArgumentException("Unsupported scheme " + uri.getScheme());
    }

    private static Flux<String> fromFile(Path path) {
        return Flux.using(() -> Files.lines(path),  // 1) generate the disposable resource
                Flux::fromStream,                   // 2) create a Flux<String> from a disposable resource from 1)
                BaseStream::close                   // 3) when Flux completes, we close the disposable resource
        ).doOnCancel(() -> LOGGER.warn("cancelled"));
    }
    public static Flux<String> fromWeb(URI uri) {
        return HttpClient.create()
                .get()
                .uri(uri.toString())
                .responseContent()
                    .asString(Charset.forName("utf-8"))
                    .concatMap(s -> Flux.fromArray(s.split("\n")))
                    .filter(s -> !s.isEmpty());
    }

    public static Flux<String> fromConsole() {
        return Flux
                .create(console());
    }

    private static Consumer<FluxSink<String>> console() {
        return sink -> {
            Scanner scanner = new Scanner(System.in);
            for (String line = scanner.nextLine(); scanner.hasNextLine(); line = scanner.nextLine()) {
                sink.next(line);
            }
            sink.complete();
        };
    }

}

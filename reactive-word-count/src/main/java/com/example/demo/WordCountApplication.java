package com.example.demo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.rabbitmq.*;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.BaseStream;

@SpringBootApplication
@RestController
public class WordCountApplication {

	public static final String ALPHANUMERIC = "^[a-zA-Z0-9]+$";

	private Logger log = LoggerFactory.getLogger(WordCountApplication.class);

	@Autowired
	RabbitMQConfiguration config;

	public static void main(String[] args) {
		SpringApplication.run(WordCountApplication.class, args);
	}

	@EventListener
	public void applicationStarted(ApplicationStartedEvent event) {
		Sender sender = config.createSender();

		// Declare required AMQP resources (queues, exchanges and bindings)
		WordCountResources resources = new WordCountResources(sender);

		// Receive lines and print out count words
		config.createReceiver()
				.consumeNoAck(resources.wordCountInputQueue)
				.delaySubscription(resources.declare())
				.flatMap(l -> Flux.fromArray(new String(l.getBody()).split("\\b")))
				.filter(w -> w.matches(ALPHANUMERIC))
				.map(String::toLowerCase)
				.groupBy(w -> w)
				.flatMap(g -> g.scan(Tuples.of(g.key(), new LongAdder()),
						(t, w) -> { t.getT2().increment();return t;} ))
				.subscribe(t -> System.out.printf("%s : %d\n", t.getT1(), t.getT2().longValue()));
	}


}

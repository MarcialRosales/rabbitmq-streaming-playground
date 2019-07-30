package com.example.demo;

import com.rabbitmq.client.Delivery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.*;
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

		// Receive lines, emit word counts
		Flux<Tuple2<String, LongAdder>> wordCounts = config.createReceiver()
				.consumeNoAck(resources.wordCountInputQueue)
				.delaySubscription(resources.declare())
				.transform(wordCount());
				
		// .. and send those emitted word counts
		sender
				.send(wordCounts.map(wc -> resources.toWordCountQueue(wc)))
				.subscribe();
	}

	private Function<Flux<Delivery>, Flux<Tuple2<String, LongAdder>>> wordCount() {
		return f -> f.flatMap(l -> Flux.fromArray(new String(l.getBody()).split("\\b")))
				.filter(w -> w.matches(ALPHANUMERIC))
				.map(String::toLowerCase)
				.groupBy(words -> words)
				.flatMap(emitWordCount());
	}
	private Function<? super GroupedFlux<String, String>, Flux<Tuple2<String, LongAdder>>> emitWordCount() {
		return g -> g.scan(Tuples.of(g.key(), new LongAdder()),
				(t, w) -> {
					t.getT2().increment();
					return t;
				});
	}
	private void printWordCounts(Tuple2<String, LongAdder> wordCount) {
		System.out.printf("%s : %d\n",
				wordCount.getT1(),
				wordCount.getT2().longValue());
	}


}

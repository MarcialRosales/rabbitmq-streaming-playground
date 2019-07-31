package com.example.wordcount;

import com.rabbitmq.client.Delivery;
import io.lettuce.core.api.reactive.RedisStringReactiveCommands;
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

import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import java.util.function.Supplier;

@SpringBootApplication
@RestController
public class WordCountApplication {


	private Logger log = LoggerFactory.getLogger(WordCountApplication.class);

	@Autowired
	RabbitMQConfiguration rabbit;
    @Autowired
    RedisStringReactiveCommands<String,String> reactiveCommands;

    public static void main(String[] args) {
		SpringApplication.run(WordCountApplication.class, args);
	}

	@EventListener
	public void applicationStarted(ApplicationStartedEvent event) {
		Sender sender = rabbit.createSender();

		// Declare required AMQP resources (queues, exchanges and bindings)
		WordCountResources resources = new WordCountResources(sender);

		// Receive lines from wordcount-input queue, and emit live word counts
		Flux<Tuple2<String, LongAdder>> wordCounts = rabbit
				.createReceiver()
				.consumeNoAck(resources.wordCountInputQueue)
				.delaySubscription(resources.declare())
				.transform(wordCount());
				
		// .. and send those emitted word counts to a word count output queue
		sender
				.send(wordCounts.map(resources::messageToWordCountQueue))
				.subscribe();

		rabbit
                .createReceiver()
                .consumeNoAck(resources.wordCountOutputQueue)
                .delaySubscription(resources.declare())
                .map(this::toWordCount)
                .flatMap(this::toRedis)
                .subscribe();


	}
	private Mono<String> toRedis(Tuple2<String, String> wc) {
	    return reactiveCommands.set(wc.getT1(), wc.getT2());
    }
    private Tuple2<String, String> toWordCount(Delivery d) {
	    String[] split = new String(d.getBody()).split(":");
	    return Tuples.of(split[0], split[1].trim());
    }

    public static final String ALPHANUMERIC = "^[a-zA-Z0-9]+$";
	private Function<Flux<Delivery>, Flux<Tuple2<String, LongAdder>>> wordCount() {
		return f -> f.flatMap(l -> Flux.fromArray(new String(l.getBody()).split("\\b")))
				.filter(w -> w.matches(ALPHANUMERIC))
				.map(String::toLowerCase)
				.groupBy(words -> words)
				.flatMap(emitWordCount());
	}
	private Function<? super GroupedFlux<String, String>, Flux<Tuple2<String, LongAdder>>> emitWordCount() {
		return g -> g.scanWith(initialWordCount(g.key()),
				(t, w) -> {
					t.getT2().increment();
					return t;
				});
	}
	private Supplier<Tuple2<String, LongAdder>> initialWordCount(String key) {
		return () -> findMonoWordCount(key).blockOptional().orElse(Tuples.of(key, new LongAdder()));
	}


	private Mono<Tuple2<String, LongAdder>> findMonoWordCount(String key) {
		return reactiveCommands.get(key).map(v -> {
			LongAdder la = new LongAdder();
			la.add(Long.valueOf(v.trim()));
			return Tuples.of(key, la);
		});
	}

}

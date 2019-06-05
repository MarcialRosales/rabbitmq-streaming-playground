package com.example.demo;

import com.rabbitmq.client.Delivery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.EventListener;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.rabbitmq.*;
import reactor.util.function.Tuple2;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.BaseStream;

@SpringBootApplication
@RestController
public class ReactiveAmqpDemoApplication {

	private Logger log = LoggerFactory.getLogger(ReactiveAmqpDemoApplication.class);

	@Autowired
	org.springframework.amqp.rabbit.connection.ConnectionFactory springConnectionFactory;

	@Autowired
	com.rabbitmq.client.ConnectionFactory connectionFactory;

	@Autowired
	SenderOptions senderOptions;

	@Autowired
	ReceiverOptions receiverOptions;

	@Bean
	public SenderOptions senderOptions() {
		return withSenderOptions(connectionFactory);
	}

	@Bean
	public ReceiverOptions receiverOptions() {
		return withReceiverOptions(connectionFactory);
	}

	@Bean
	com.rabbitmq.client.ConnectionFactory connectionFactory() {
		CachingConnectionFactory ccf = (CachingConnectionFactory) springConnectionFactory.getPublisherConnectionFactory();
		return ccf.getRabbitConnectionFactory();
	}

	@GetMapping(path = "/wordcounts", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
	public Flux<String> wordCounts() {
		return consumeWordCounts().map(wordCountToString());
	}

	private Function<Delivery, String> wordCountToString() {
		return delivery -> String.format("%s : %s", delivery.getEnvelope().getRoutingKey(), new String(delivery.getBody()));
	}

	private Flux<Delivery> consumeWordCounts() {
		Mono<?> untilWordCountOutputExists = declareWordCountOutput(RabbitFlux.createSender(senderOptions));

		return RabbitFlux.createReceiver(receiverOptions)
				.consumeAutoAck(wordCountOutputQueue().getName())
				.delaySubscription(untilWordCountOutputExists);
	}

	@GetMapping("/send")
	public void sendMore() {
		String inputFile = "src/main/resources/simpler-wordcount.txt";

		// Create sender of sentences to wordcount-input exchange
		Sender linesSender = RabbitFlux.createSender(senderOptions);
		Mono<?> untilWordCountInputExists = declareWordCountInput(linesSender);

		// Read from file and send lines to wordcount-input exchange
		linesSender
				.send(readLinesFromFile(inputFile)
						.map(stringToMessageForExchange(wordCountInput(), bindWordCountInputQueue().getRoutingKey()))
						.delayElements(Duration.ofMillis(100))
						.doOnComplete(() -> System.out.println("sent all lines"))
				)
				.delaySubscription(untilWordCountInputExists)
				.block();
	}

	@EventListener
	public void applicationStarted(ApplicationStartedEvent event) {

		// Create local table
		FluxTableLongCounter<String> wordCountTable = new FluxTableLongCounter<>();

		// Create sender of sentences to wordcount-input exchange
		Sender linesSender = RabbitFlux.createSender(senderOptions);
		Mono<?> untilWordCountInputExists = declareWordCountInput(linesSender);

		// Create sender of live word counts to wordcount-output exchange
		Sender wordCountSender = RabbitFlux.createSender(senderOptions);
		Mono<?> untilWordCountOutputExist = declareWordCountOutput(wordCountSender);

		// Receive lines and count words using wordCounter table
		consumeFrom(wordCountInputQueue())
				.delaySubscription(untilWordCountInputExists)
				.subscribe(countWordsUsing(wordCountTable));

		// Stream live word counts (coming from the local table) to the wordcount-output exchange
		wordCountSender
				.send(wordCountTable.toStream().log().map(mapEntryToMessageFor(wordCountOutput())))
				.delaySubscription(untilWordCountOutputExist)
				.subscribe();

	}

	private void topTenWordCount() {
		Sender sender = RabbitFlux.createSender(senderOptions);
		Mono<?> untilWordCountOutputExist = declareWordCountOutput(sender);

		// Create local table
		ConcurrentSkipListMap table = new ConcurrentSkipListMap();

		consumeFrom(wordCountOutputQueue())
				.delaySubscription(untilWordCountOutputExist)
				.doOnNext(count(table))
				.subscribe(AcknowledgableDelivery::ack);

	}

	private Consumer<AcknowledgableDelivery> aggregate(FluxTableLongCounter<String> wordCountTable) {
		return (delivery) ->
		{
			Flux.fromArray(new String(delivery.getBody()).split("\\b"))
					.filter(alphaNumericWords()).map(String::toLowerCase)
					.groupBy(w -> w)
					.flatMap(g -> Flux.just(g.key()).zipWith(g.count()))
					.subscribe(
							wordCountTable.count(),
							(throwable) -> delivery.nack(false),
							delivery::ack);

		};
	}

	public static void main(String[] args) {
		SpringApplication.run(ReactiveAmqpDemoApplication.class, args);
	}


	private Flux<String> fromPath(Path path) {
		return Flux.using(() -> Files.lines(path),
				Flux::fromStream,
				BaseStream::close
		).doOnCancel(() -> log.warn("cancelled"));
	}

	private ReceiverOptions withReceiverOptions(com.rabbitmq.client.ConnectionFactory connectionFactory) {
		return new ReceiverOptions()
				.connectionFactory(connectionFactory)
				.connectionSubscriptionScheduler(Schedulers.elastic());
	}

	private SenderOptions withSenderOptions(com.rabbitmq.client.ConnectionFactory connectionFactory) {
		SenderOptions senderOptions = new SenderOptions()
				.connectionFactory(connectionFactory)
				.resourceManagementScheduler(Schedulers.elastic());
		return senderOptions;
	}

	private ExchangeSpecification wordCountInput() {
		return ExchangeSpecification.exchange("wordcount-input").durable(true).type("direct");
	}

	private QueueSpecification wordCountInputQueue() {
		return QueueSpecification.queue("wordcount-input-q").durable(true);
	}

	private QueueSpecification wordCountOutputQueue() {
		return QueueSpecification.queue("wordcount-output-q").durable(true);
	}

	private BindingSpecification bindWordCountInputQueue() {
		return BindingSpecification.binding(wordCountInput().getName(), "lines", wordCountInputQueue().getName());
	}

	private BindingSpecification bindWordCountOutputQueue() {
		return BindingSpecification.binding(wordCountOutput().getName(), "#", wordCountOutputQueue().getName());
	}

	private ExchangeSpecification wordCountOutput() {
		return ExchangeSpecification.exchange("wordcount-output").durable(true).type("topic");
	}


	private Mono<?> declareWordCountInput(Sender sender) {
		return
				sender.declareQueue(wordCountInputQueue())
						.then(sender.declareExchange(wordCountInput()))
						.then(sender.bind(bindWordCountInputQueue()));
	}

	private Mono<?> declareWordCountOutput(Sender sender) {
		return
				sender.declareExchange(wordCountOutput())
						.then(sender.declareQueue(wordCountOutputQueue()))
						.then(sender.bind(bindWordCountOutputQueue()));
	}

	private Flux<String> readLinesFromFile(String filename) {
		return fromPath(Paths.get(filename))
				.filter(s -> !s.startsWith("#"));
	}

	private Flux<AcknowledgableDelivery> consumeFrom(QueueSpecification queue) {
		return RabbitFlux.createReceiver(withReceiverOptions(connectionFactory))
				.consumeManualAck(queue.getName());
	}

	private Consumer<AcknowledgableDelivery> countWordsUsing(FluxTableLongCounter<String> wordCountTable) {
		return (delivery) ->
		{
			Flux.fromArray(new String(delivery.getBody()).split("\\b"))
					.filter(alphaNumericWords()).map(w -> w.toLowerCase())
					.groupBy(w -> w)
					.flatMap(g -> Flux.just(g.key()).zipWith(g.count()))
					.subscribe(
							wordCountTable.count(),
							(throwable) -> delivery.nack(false),
							() -> delivery.ack());

		};
	}

	private Function<Map.Entry<String, AtomicLong>, OutboundMessage> mapEntryToMessageFor(ExchangeSpecification exchange) {
		return kv -> new OutboundMessage(
				exchange.getName(),
				kv.getKey(),
				String.valueOf(kv.getValue()).getBytes());
	}

	private Function<String, OutboundMessage> stringToMessageForExchange(ExchangeSpecification exchange, String routingKey) {
		return (String v) -> new OutboundMessage(
				exchange.getName(),
				routingKey,
				v.getBytes());
	}

	private Predicate<? super String> alphaNumericWords() {
		return w -> w.matches("^[a-zA-Z0-9]+$");
	}


	class FluxTable<K, V> {
		protected Map<K, V> table;
		protected EmitterProcessor<Map.Entry<K, V>> tableEmitter;
		protected FluxSink<Map.Entry<K, V>> sink;

		public FluxTable() {
			table = new HashMap<>();
			tableEmitter = EmitterProcessor.create(1);
			sink = tableEmitter.sink();
		}

		public Flux<Map.Entry<K, V>> toStream() {
			return tableEmitter;
		}

		public Consumer<? super Tuple2<K, V>> store() {
			return (tuple) -> {
				put(tuple.getT1(), tuple.getT2());
			};
		}

		private void put(K key, V value) {
			table.put(key, value);
			sink.next(new AbstractMap.SimpleEntry(key, value));
		}

	}

	class FluxTableLongCounter<K> extends FluxTable<K, AtomicLong> {

		public Long count(K key, Long value) {
			table.computeIfAbsent(key, s -> new AtomicLong(0));
			return table.get(key).addAndGet(value);
		}

		public Consumer<? super Tuple2<K, Long>> count() {
			return (tuple) -> {
				sink.next(new AbstractMap.SimpleEntry(tuple.getT1(), count(tuple.getT1(), tuple.getT2())));
			};
		}
	}
	public Consumer<AcknowledgableDelivery> count(ConcurrentSkipListMap<String, AtomicLong> table) {
		return (d) -> {
			table.count(d.getEnvelope().getRoutingKey(), Long.parseLong(new String(d.getBody())));
		};
	}
}

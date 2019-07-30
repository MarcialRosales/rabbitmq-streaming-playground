package com.example.demo;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.ConnectionFactory;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.rabbitmq.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.BaseStream;

public class OtherBeans {

    private Logger log = LoggerFactory.getLogger(WordCountApplication.class);



    @Bean
    @ConditionalOnProperty("sendSampleFile")
    public CommandLineRunner runner() {
        return (args) -> {

            ConnectionFactory connectionFactory = new ConnectionFactory();
            connectionFactory.useNio();

            consume(connectionFactory, sampleQueue(), msg -> log.info("Received {}", msg));

            send(connectionFactory, sampleExchange(),
                    generateMessagesFromPath(Paths.get("src/main/resources/simpler-wordcount.txt"))
                            .delayElements(Duration.ofMillis(50))  // at a rate of 50 msec
            );


            // Useful
            //Flux.interval(Duration.ofMillis(1000)).subscribe(t -> log.info("Ticket {}", t));
        };

    }
    private Flux<OutboundMessage> generateMessagesFromPath(Path path) {
        return fromPath(path)
                .filter(s -> !s.startsWith("#"))
                .map(line -> new OutboundMessage(
                        "amq.direct",
                        "routing.key", line.getBytes()));
    }

    private Flux<String> fromPath(Path path) {
        return Flux.using(() -> Files.lines(path),
                Flux::fromStream,
                BaseStream::close
        ).doOnCancel(() -> log.warn("cancelled"));
    }

    private ReceiverOptions withReceiverOptions(ConnectionFactory connectionFactory) {
        return new ReceiverOptions()
                .connectionFactory(connectionFactory)
                .connectionSubscriptionScheduler(Schedulers.elastic());
    }

    private SenderOptions withSenderOptions(ConnectionFactory connectionFactory) {
        SenderOptions senderOptions =  new SenderOptions()
                .connectionFactory(connectionFactory)
                .resourceManagementScheduler(Schedulers.elastic());
        return senderOptions;
    }

    private QueueSpecification sampleQueue() {
        return QueueSpecification.queue("site-1").durable(true);
    }
    private ExchangeSpecification sampleExchange() {
        return ExchangeSpecification.exchange("amq.direct").durable(true);
    }
    private ExchangeSpecification wordCountInput() {
        return ExchangeSpecification.exchange("wordcount-input").durable(true).type("direct");
    }
    private QueueSpecification wordCountInputQueue() {
        return QueueSpecification.queue("wordcount-input-q").durable(true);
    }
    private BindingSpecification wordCountInputWithInputQueue() {
        return BindingSpecification.binding(wordCountInput().getName(), "*", wordCountInputQueue().getName());
    }
    private ExchangeSpecification wordCountOutput() {
        return ExchangeSpecification.exchange("wordcount-output").durable(true).type("topic");
    }

    private Disposable consume(ConnectionFactory connectionFactory, QueueSpecification queue, java.util.function.Consumer<? super String> consumer) {
        Publisher<AMQP.Queue.DeclareOk> untilQueueExists = RabbitFlux.createSender(withSenderOptions(connectionFactory)).declareQueue(queue);

        return RabbitFlux.createReceiver(withReceiverOptions(connectionFactory))
                .consumeManualAck(queue.getName())
                .delaySubscription(untilQueueExists)
                .subscribe(delivery ->
                {
                    Flux.fromArray(new String(delivery.getBody()).split("\\b"))
                            .filter(w -> w.matches("^[a-zA-Z0-9]+$"))
                            .map(w -> w.toLowerCase())
                            .groupBy(w -> w)
                            .flatMap(group -> Mono.just(group.key()).zipWith(group.count()))	// block until we have counted all words from all 20 messages
                            .map(keyAndCount -> keyAndCount.getT1() + " => " + keyAndCount.getT2())
                            .subscribe(System.out::println,
                                    (throwable) -> delivery.nack(false),
                                    () -> delivery.ack());

                });

    }
    private Disposable consumeInBatches(ConnectionFactory connectionFactory, QueueSpecification queue, java.util.function.Consumer<? super String> consumer) {
        Publisher<AMQP.Queue.DeclareOk> untilQueueExists = RabbitFlux.createSender(withSenderOptions(connectionFactory)).declareQueue(queue);

        return RabbitFlux.createReceiver(withReceiverOptions(connectionFactory))
                .consumeManualAck(queue.getName())
                .delaySubscription(untilQueueExists)
                .bufferTimeout(20, Duration.ofSeconds(5))	// process (and ack) messages in batches of up to 20 mgs
                .subscribe(deliveries ->
                {
                    Flux.fromIterable(deliveries)
                            .flatMap(delivery -> Flux.fromArray(new String(delivery.getBody()).split("\\b")))
                            .filter(w -> w.matches("^[a-zA-Z0-9]+$"))
                            .map(w -> w.toLowerCase())
                            .groupBy(w -> w)
                            .flatMap(group -> Mono.just(group.key()).zipWith(group.count()))	// block until we have counted all words from all 20 messages
                            .map(keyAndCount -> keyAndCount.getT1() + " => " + keyAndCount.getT2())
                            .subscribe(System.out::println,
                                    (throwable) -> deliveries.get(deliveries.size()-1).nack(true, false),
                                    () -> deliveries.get(deliveries.size()-1).ack(true));

                });

    }

    private Disposable send(ConnectionFactory connectionFactory, ExchangeSpecification exchange, Flux<OutboundMessage> outboundFlux) {

        Sender sender = RabbitFlux.createSender(withSenderOptions(connectionFactory));
        Publisher<AMQP.Exchange.DeclareOk> untilExchangeExists = sender.declareExchange(exchange);

        return sender
                .send(outboundFlux)
                .delaySubscription(untilExchangeExists)
                .doOnSuccess(m -> log.info("Sent message "))
                .doOnError(e -> log.error("Send failed", e))
                .subscribe();		// Start pulling messages to send
    }

    public static void main(String[] args) {
        SpringApplication.run(WordCountApplication.class, args);
    }


    @Bean
    @ConditionalOnProperty("understandingFlatmap")
    public CommandLineRunner understandingFlatmap() {

        return (args) -> {

            Flux<Long> numbers = Flux.just(0L, 1L, 2L, 3L);

            Flux
                    .just(0L)
                    .flatMap( i -> numbers.map(j -> j*2))
                    .subscribe(t -> log.info("{}", t));

        };
    }

    @Bean
    @ConditionalOnProperty("understandScanning")
    public CommandLineRunner understandScanning() {

        return (args) -> {
            Flux.just(3, 5, -2, 9)
                    .scan(0, (totalSoFar, currentValue) -> {
                        //	log.info("TotalSoFar={}, currentValue={}", totalSoFar, currentValue);
                        return totalSoFar + currentValue;
                    }).subscribe(System.out::println);

        };

    }

    @Bean
    @ConditionalOnProperty("understandWindow")
    public CommandLineRunner understandWindow() {

        return (args) -> {
            List<Integer> values = Flux.range(1, 100).collectList().block();
            Collections.shuffle(values);

            Semaphore sem = new Semaphore(0);

            Flux.fromIterable(values)
                    .delayElements(Duration.ofMillis(500))
                    .window(10)		// take 10 elements at a time into a flux and pass them right away rather than accumulating them like
                    // buffer operator does
                    .flatMap(w -> w.reduce(0, (a, b) -> {return a+b;}))  // convert a flux of 10 into a flux of Mono with the sum
                    .doOnComplete(() -> sem.release())
                    .subscribe(x -> System.out.println(Thread.currentThread().getName() + " => " + x));

            sem.acquire();

        };

    }


    @Bean
    @ConditionalOnProperty("understandConcatMap")
    public CommandLineRunner understandConcatMap() {

        return (args) -> {
            List<Integer> values = Flux.range(1, 100).collectList().block();
            Collections.shuffle(values);

            Flux.fromIterable(values)
                    .log()
                    .groupBy(v -> v % 4)
                    .concatMap(x-> {
                        System.out.println("module " + x.key());
                        return x;
                    })
                    .subscribe(x -> System.out.println(x));


            // concatMap waits until the received Flux(s) have completed to then emit the result from those Flux
        };

    }


    @Bean
    @ConditionalOnProperty("understandReduce")
    public CommandLineRunner understandReduce() {

        return (args) -> {


            Flux.just(3, 5, -2, 9, 1, 5, 4, 6)
                    .buffer(2)
                    .flatMap(b -> Flux.fromIterable(b).reduce(0, (totalSoFar, val) -> {
                        //	log.info("totalSoFar={}, emitted={}", totalSoFar, val);
                        return totalSoFar + val;
                    }))
                    .subscribe(System.out::println);;

        };

    }


    @Bean
    @ConditionalOnProperty("understandingGroupBy")
    public CommandLineRunner understandingGroupBy() {

        return (args) -> {
/*
			Flux.just("the", "you", "their", "the",  "their", "you", "I")
					.doOnRequest(lc-> System.out.println("> onRequest-flux" ))
					.doOnNext(w -> System.out.println(" - onNext-just " + w))
					.doOnComplete(() -> System.out.println(" < onComplete-just"))
					.groupBy(w -> w, 2)

					.doOnNext(g -> System.out.println(" - onNext-groupBy " + g.key()))
					.doOnComplete(() -> System.out.println(" < onComplete-groupBy"))
					.doOnRequest(lc-> System.out.println("> onRequest-groupBy" ))

					.flatMap(g -> Mono.just(g.key()).zipWith(g.count())).doOnNext(c -> System.out.println(" - onNext-flatMap " + c))

					.subscribe(System.out::println);
*/

            List<Integer> values = Flux.range(1, 100).collectList().block();
            Collections.shuffle(values);
/*

			Flux.
					fromIterable(values)
					.buffer(10)
					.flatMap(b -> Flux.fromIterable(b).groupBy(v -> v % 2))
					.flatMap(v -> Mono.just(v.key()).zipWith(v.count()))
					.subscribe(z -> System.out.println(z.getT2() + " => " + z.getT1()));
*/



            Map<String, AtomicLong> counter = new HashMap<>();

            Flux.
                    fromIterable(values)
                    .groupBy(v -> v % 4)
                    .map(g -> g.publishOn(Schedulers.parallel()))
                    .subscribe(stream ->
                            stream.buffer(10).subscribe(b -> {
                                        long acc = counter.computeIfAbsent(Thread.currentThread().getName(),
                                                (k) -> {return new AtomicLong();}).addAndGet(b.size());
                                        System.out.println(Thread.currentThread().getName() + " delta:" + b.size() + " total:" + acc) ;

                                    }
                            ));


        };
    }

    @Bean
    @ConditionalOnProperty("wordCount")
    public CommandLineRunner wordCount() {

        return (args) -> {

            //fromPath(Paths.get("src/main/resources/wordcount.txt")).take(10).subscribe(System.out::println);

            fromPath(Paths.get("src/main/resources/simpler-wordcount.txt"))
                    .flatMap(line -> Flux.fromArray(line.split("\\b"))
                            .filter(w -> w.matches("^[a-zA-Z0-9]+$"))
                            .map(w -> w.toLowerCase()))
                    .groupBy(w -> w)
                    //.log()
                    .flatMap(group -> Mono.just(group.key()).zipWith(group.count()))
                    .map(keyAndCount -> keyAndCount.getT1() + " => " + keyAndCount.getT2())
                    //.log()
                    .parallel(2)
                    .runOn(Schedulers.parallel())
                    .subscribe(v -> log.info("{}", v));


        };
    }

    @Bean
    @ConditionalOnProperty("windowedWordCount")
    public CommandLineRunner windowedWordCount() {

        return (args) -> {


            fromPath(Paths.get("src/main/resources/simpler-wordcount.txt"))
                    .buffer(10).map(l -> Flux.fromIterable(l))
                    .flatMap(w -> w)
                    .flatMap(line -> Flux.fromArray(line.split("\\b"))
                            .filter(w -> w.matches("^[a-zA-Z0-9]+$"))
                            .map(w -> w.toLowerCase()))
                    .groupBy(w -> w, 10)
                    //.log()
                    .flatMap(group -> Mono.just(group.key()).zipWith(group.count()))
                    .map(keyAndCount -> keyAndCount.getT1() + " => " + keyAndCount.getT2())
                    //.log()
                    .subscribe(System.out::println);



        };
    }


    @Bean
    @ConditionalOnProperty("simulatingReadingUpMost10RecordsFromDb")
    public CommandLineRunner simulatingReadingUpMost10RecordsFromDb() {

        return (args) -> {

            Flux<String> flux = Flux.generate(
                    AtomicLong::new,		// useful to open a database connection
                    (state, sink) -> {		// here we read database records
                        long i = state.getAndIncrement();
                        sink.next("3 x " + i + " = " + 3*i);
                        //if (i == 10) sink.complete();
                        return state;
                    },
                    (s) -> log.info("Completing generation with state {}", s));	// here we could close the db connection
            flux.log()
                    .limitRequest(10)		// we only care to read 10 records
                    .subscribe();

        };
    }


}

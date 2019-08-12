package com.example;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

@Slf4j
public class PlayingWithGenerators {

    @Test
    public void backToBasics() {
        // [consumer:thread-N]<--sink(no-buffer)---[generator:thread-N]
        Flux<String> flux = synchronousGenerator(10).log();

        flux.subscribe(v -> System.out.println("We got " + v));

    }

    private  Flux<String> synchronousGenerator(int generateUpTo) {
        return Flux.generate(
                AtomicLong::new,
                (state, sink) -> {  // here sink is a SynchronousSink
                    long i = state.getAndIncrement();
                    // synchronous because this method can only be called once per callback
                    // in other words this generator can only emit one element at a time
                    sink.next("3 x " + i + " = " + 3*i);
                    if (i == generateUpTo) sink.complete();
                    return state;
                }, (state) -> System.out.println("We are done. This is the last state: " + state));
    }


    interface MyEventListener {
        void onMessage(String message);
        void processComplete(); // e.g. cancelled subscription
    }
    class Rabbit {
        MyEventListener listener;
        void register(MyEventListener listener) {
            log.info("subscribed!");
            this.listener = listener;
        }
        public void messageArrived(String message) {
            listener.onMessage(message);
        }
        public void subscriptionCanceled() {
            log.info("unsubscribed!");
            listener.processComplete();
        }
    }

    @Test
    public void createFluxLikeTheRabbitMqReceiverWithoutNoBackPressure() throws InterruptedException {

        // this type of emitter/generator allows us to emit multiple elements per round, even from
        // multiple threads. [consumer:thread-N]<--sink(buffer)---[generator:thread-M]
        // the issue is that the generator is free to call .next on the sink as fast as it can
        // therefore the only way to slow it down is by putting some throttling in the sink itself.

        // Careful not to block in the create lambda
        Rabbit rabbit = new Rabbit();

        Flux<String> messageStream = Flux.create(sink -> { // here sink is a FluxSink rather than a SynchronousSink
            rabbit.register(new MyEventListener() {
                @Override
                public void onMessage(String message) {
                    sink.next(message);
                }

                @Override
                public void processComplete() {
                    sink.complete();
                }
            });
        }, FluxSink.OverflowStrategy.LATEST); // change it to BUFFER, or DROP

        CountDownLatch done = new CountDownLatch(1);
        LongAdder receivedMessages  = new LongAdder();

        messageStream
                .log()
                .delayElements(Duration.ofMillis(100))
                .doOnNext(m -> receivedMessages.increment())
                .subscribe(m -> log.info(m), e -> log.error("Error", e),
                        () -> {done.countDown(); log.info("done!  received {}", receivedMessages);});

        // Simulate arrival of 100 messages
        Flux.range(1, 100)
                .map(String::valueOf)
                .doOnNext(v -> rabbit.messageArrived(v))
                .doOnComplete(()-> {log.info("Generated all messages"); rabbit.subscriptionCanceled();})
                .subscribe();

        done.await();


        // In the output we will see that we have generated all 100 messages
        // and our subscriber is about to receive the first !! All this messages are in an unbounded buffer
        // this strategy is BUFFER (i.e. buffer everything)

        // 12:33:46.876 [main] INFO com.example.PlayingWithGenerators - Generated all messages
        // 12:33:46.974 [parallel-1] INFO com.example.PlayingWithGenerators - 1

        // however there is back-pressure from the sink downwards to the pipeline. It is requesting 32
        // elements at a time
    }
}

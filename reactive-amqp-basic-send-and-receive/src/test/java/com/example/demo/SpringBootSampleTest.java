package com.example.demo;

import net.bytebuddy.implementation.bytecode.Throw;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.retry.Repeat;
import reactor.retry.Retry;

import java.io.IOException;
import java.rmi.MarshalledObject;
import java.time.Duration;
import java.time.temporal.TemporalUnit;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static java.time.Duration.ofMillis;

import static org.junit.Assert.*;

public class SpringBootSampleTest {

    @Test
    public void nullValuesShouldNeverBeProgagated() {
        String source = null;

        Mono.justOrEmpty(source)
                .subscribe(System.out::println);

        Mono.fromFuture( new CompletableFuture<>())
                .or(Mono.just("hello"))
                .subscribe(System.out::println);

    }
    @Test
    public void anErrorTerminatesTheStream() {

        Mono.from(
                    Mono.error(new RuntimeException("some error occurred"))
                ).onErrorMap(t -> new RuntimeException("Wrapper exception. Originator was " + t.toString(), t))
                 .subscribe(
                        System.out::println,    // data/event channel
                        System.err::println // error channel
                );

    }
    @Test
    public void shouldRevertToAnotherStreamIfMainStreamHasNoValue() {
        CompletableFuture<String> someFutureValue = new CompletableFuture<>();
        someFutureValue.complete("some value");

        Mono.fromFuture(someFutureValue)
                .or(Mono.just("some other value"))
                .doOnNext(System.out::println)
                .subscribe();

    }
    @Test
    public void runFutureForSometimeAndThenTerminateWithoutResult() {
        CompletableFuture<String> someFutureValue = new CompletableFuture<>();

        Mono.fromFuture(someFutureValue)
                .take(Duration.ofSeconds(1))
                .doOnNext(System.out::println)
                .doOnTerminate(() -> System.out.println("Terminated"))
                .block();

    }
    @Test
    public void runFutureForSometimeAndThenTerminateWithOtherResult() {
        CompletableFuture<String> someFutureValue = new CompletableFuture<>();

        Mono.fromFuture(someFutureValue)
                .take(Duration.ofSeconds(1))
                .thenReturn("some fixed value")
                .doOnNext(System.out::println)
                .doOnTerminate(() -> System.out.println("Terminated"))
                .block();

    }
    @Test
    public void waitForMonoAndThenContinueWithOthers() {
        CompletableFuture<String> someFutureValue = new CompletableFuture<>();

        Mono.just("some connection")
                .delayElement(Duration.ofSeconds(1))
                .thenEmpty(Mono.just("another value").doOnSuccess(System.out::println).then(Mono.empty()))
                .then(Mono.just("other value"))
                .doOnNext(System.out::println)
                .doOnTerminate(() -> System.out.println("Terminated"))
                .block();

    }

    @Test
    public void useDifferentSubscribersAtDifferentStages() {
        Flux.range(1, 10)
                    .doOnNext(v -> System.out.println("Subscriber 1 : "+  v))
                    .map(String::valueOf)
                    .buffer(2)
                    .doOnNext(v -> System.out.printf("Subscriber 2 : with %d elements\n", v.size()) )
                    .subscribe();


    }

    private Mono callAPI() {
        return Mono.defer(() -> {
            System.out.println("API call @ " + System.currentTimeMillis());
            return Mono.delay(Duration.ofMillis(500)).error(new RuntimeException("Some api call exception"));
        });
    }
    class CallAPISupplier implements Supplier<Mono<String>> {

        int failFirst;

        public CallAPISupplier(int failFirst) {
            this.failFirst = failFirst;
        }

        @Override
        public Mono<String> get() {
            return failFirst-- > 0 ?
                    Mono.delay(Duration.ofMillis(500)).error(new RuntimeException("Some api call exception"))
                    :
                    Mono.just("some connection");
        }
    }
    private Mono<String> callAPI(int failFirst) {
        return Mono.defer(new CallAPISupplier(failFirst));
    }


    @Test
    public void testingWithoutRetry() {

        // Calling api without retry
        callAPI()
                .doOnError(System.err::println)
                .subscribe();


    }
    @Test
    public void testingWithBasicRetry() {


        callAPI()
                .retry(4)
                .doOnError(System.err::println)
                .subscribe();



    }
    @Test
    public void testingWithExpBackOffRetry() {

        // After exhausted all retries, it fails and we can deal with exception in two places: at //1 and at //2
        try {
            callAPI().retryBackoff(4, ofMillis(100), ofMillis(200))
                    .doOnError(t -> System.err.println("An error occurred " + t)) // 1
                    .doOnTerminate(() -> System.out.println("Terminated"))
                    .block();
        }catch(IllegalStateException e) {
            System.err.printf("Finished badly due to %s. Root Cause ", e.getMessage(), e.getCause() ); // 2
        }

    }

    @Test
    public void testWithExpBackOffRetryWithExceptionFilter() {
        // Retry.any();    // retry should any exception occurred
        // Retry.anyOf(IOException.class); // retry should IOException occurred

        Object someAppContext = "Context";

        Retry retryPolicy = Retry.any()
                .randomBackoff(Duration.ofMillis(100), Duration.ofSeconds(2))
                .withApplicationContext(someAppContext)
                .doOnRetry(context -> {
                    System.out.printf("Failed Attempt %d due to [%s] (delay: %dmsec). appContext [%s]\n",
                            context.iteration(),
                            context.exception().getMessage(),
                            context.backoff().toMillis(),
                            context.applicationContext());
                })
                .timeout(Duration.ofSeconds(20))
                .retryMax(7);

        Mono<String> conn = callAPI(3)
                .retryWhen(retryPolicy)
                .cache()
                .doOnNext(v -> System.out.printf("Finally Received %s\n ", v));
        conn.subscribe(s -> { System.out.println("Received connection " + s);});

        conn.subscribe(s -> { System.out.println("Received connection " + s);});

        conn.block();
    }


    @Test
    public void testCache() {
        Mono coldMono = Mono.fromCallable(() -> {
            System.out.println("Producing some value");
            return "some value";
        }).cache();

        Flux.range(1, 10).subscribe(v -> coldMono.subscribe(System.out::println));

    }

    @Test
    public void anErrorTerminatesStreamByDefault() throws InterruptedException {
        final AtomicInteger counter = new AtomicInteger(0);

        Flux<String> flux =
                Flux.interval(Duration.ofMillis(250))
                        .map(input -> {
                            if (input < 3) { counter.incrementAndGet(); return "tick " + input; };
                            throw new RuntimeException("boom");
                        })
                        .onErrorReturn("Uh oh")
                .take(5);

        flux.subscribe(System.out::println);
        Thread.sleep(2100);
        assertEquals(3, counter.get());
    }
    @Test
    public void anErrorTerminatesStreamUnlessWeHandleIt() throws InterruptedException {
        final AtomicInteger counter = new AtomicInteger(0);

        Flux<String> flux =
                Flux.interval(Duration.ofMillis(250))
                        .map(input -> {
                            if (input < 3) { counter.incrementAndGet(); return "tick " + input; };
                            throw new RuntimeException("boom");
                        })
                        .onErrorReturn("Uh oh")
                        .take(5);

        flux.subscribe(System.out::println);
        Thread.sleep(2100);
        assertEquals(3, counter.get());
    }


    @Test
    public void retryAlwaysResubscribeStreamWhenErrorOccurs() throws InterruptedException {
        Flux
                .interval(Duration.ofMillis(250))
                .log()
                .doOnNext(i -> { if ((i+1) % 3 == 0) throw new RuntimeException(); } )
                .subscribe(System.out::println, System.err::println);

        Thread.sleep(250*5);

        Flux
                .interval(Duration.ofMillis(250))
                .log()
                .doOnNext(i -> { if ((i+1) % 3 == 0) throw new RuntimeException(); } )
                .retry(1)
                .subscribe(System.out::println, System.err::println);

        Thread.sleep(250*5);
    }


    // Instead, it is recommended to extend the BaseSubscriber class provided by Project Reactor
    // rather than implementing ours and make it compliant with TCK requirements for subscribers which is not trivial.
}
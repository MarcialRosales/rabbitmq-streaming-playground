/*
 * Copyright (c) 2019 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.example.demo;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.http.client.ReactorNettyClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.rabbitmq.*;

import java.time.Duration;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 *
 * IMPORTANT CONCEPT:
 * send or sendWithConfirmation subscribes to a Mono<Connection> to map it to a Mono<Channel>. This last map operation
 * creates a new channel. It is not explicitly cached. Therefore, every send invocation on a Flux of messages creates
 * a new channel. When the flux completes, it closes the channel.
 *
 */
@SpringBootApplication
public class SpringBootSample {

    public static void main(String[] args) {
        SpringApplication.run(SpringBootSample.class, args).close();
    }

    @Value("${messageCount:50}")
    int messageCount;

    @Bean
    ChaosMonkey chaosMonkey(ReactorNettyClient http) {
        return  new ChaosMonkey(http);
    }

    @Autowired
    ChaosMonkey chaosMonkey;

    @Bean
    CommandLineRunner simulateConnectionFailureWhenUsingPublisherConfirm(Sender sender, Receiver receiver, ResourceDeclaration resourceDeclaration,
        @Qualifier("senderConnection") Mono<Connection> senderConnection, //
        @Qualifier("receiverConnection") Mono<Connection> receiverConnection, //
        ReactorNettyClient http) {

        return args -> {

            // Simulate closing both connections
            //chaosMonkey.closeConnectionAfter(senderConnection, Duration.ofSeconds(2));
            chaosMonkey.closeConnectionAfter(receiverConnection, Duration.ofSeconds(5));

            CountDownLatch senderAndReceiverCompleted = new CountDownLatch(2);
            ResilientIntegerSender theSender = new ResilientIntegerSender(sender, resourceDeclaration, messageCount, senderAndReceiverCompleted);
            theSender.run();
            IntegerReceiver theReceiver = new ResilientIntegerReceiverWithRandomExceptionWhileProcessingMessage(receiver, resourceDeclaration, messageCount, senderAndReceiverCompleted);
            theReceiver.run();

            if (!senderAndReceiverCompleted.await(120, TimeUnit.SECONDS)) {
                System.err.println("Did not complete successfully");
            }else {
                System.out.println(theSender.toString());
                System.out.println(theReceiver.toString());
            }


        };
    }


    @Bean
    ResourceDeclaration resourceDeclaration(Sender sender) {
        return new ResourceDeclaration(sender);
    }
    //@Bean
    CommandLineRunner integerSender(Sender sender, ResourceDeclaration resourceDeclaration) {
        return new IntegerSender(sender, resourceDeclaration, messageCount, null);
    }

    //@Bean
    CommandLineRunner integerSenderUsingUnknownExchange(Sender sender, ResourceDeclaration resourceDeclaration) {
        return new IntegerSenderUsingUnknownExchange(sender, resourceDeclaration, messageCount, null);
    }

    //@Bean
    CommandLineRunner resilientIntegerSender(Sender sender, ResourceDeclaration resourceDeclaration) {
        return new ResilientIntegerSender(sender, resourceDeclaration, messageCount, null);
    }
    //@Bean
    CommandLineRunner resilientIntegerSender_SendingUnroutableMessage(Sender sender, ResourceDeclaration resourceDeclaration) {
        return new ResilientIntegerSender_SendingUnroutableMessage(sender, resourceDeclaration, messageCount, null);
    }




    //@Bean
    CommandLineRunner integerReceiver(Receiver receiver, ResourceDeclaration resourceDeclaration) {
        return new IntegerReceiver(receiver, resourceDeclaration, messageCount, null);
    }


}

/**
 * Messages are sent without no exceptions however RabbitMq server rejects the first message asynchronously and when
 * the RabbitMQ client receives the response, it closes the channel. So we will see the channelclosehandler being invoked.
 * That's it. All our messages have been lost.
 *
 * How do we deal with it ?
 *
 * Only in case of IOExceptions (i.e. connectivity problems), the send operation throws an exception. The default exception handler
 * provided by SendOptions retries.
 *
 */
class IntegerSenderUsingUnknownExchange extends IntegerSender {

    public IntegerSenderUsingUnknownExchange(Sender sender, ResourceDeclaration resourceDeclaration, int count,  CountDownLatch countDownWhenTerminate) {
        super(sender, resourceDeclaration, count, countDownWhenTerminate);
    }

    @Override
    protected OutboundMessage toAmqpMessage(int index) {
        if ((index % 2) == 0) {
            return new OutboundMessage("unknown-exchange", resourceDeclaration.queueName(), ("Message_" + index).getBytes());
        }else {
            return super.toAmqpMessage(index);
        }
    }


}

/**
 * Uses publisher confirmation and mandatory flag (SendOptions.trackReturned) to send messages.
 *
 */
class ResilientIntegerSender extends IntegerSender {

    public ResilientIntegerSender(Sender sender, ResourceDeclaration resourceDeclaration, int count,  CountDownLatch countDownWhenTerminate) {
        super(sender, resourceDeclaration, count, countDownWhenTerminate);
        sendOptions.trackReturned(true);
    }
    public void run(String ... args) {
        CountDownLatch deliveredAllMessages = new CountDownLatch(1);

        Flux<OutboundMessageResult>
                messageDeliveryStream = resourceDeclaration
                        .declare()
                        .thenMany(sendWithConfirmations(integers(count)));

        messageDeliveryStream
                .doOnError(System.err::println)
                .doOnTerminate(countDownWhenTerminate::countDown)
                .subscribe(this::handleMessageDelivery, System.err::println, () -> {LOGGER.info("Finished sending integers"); });
    }
    AtomicLong returnedMessageCount = new AtomicLong();
    AtomicLong ackMessageCount = new AtomicLong();
    AtomicLong nackMessageCount = new AtomicLong();

    private void handleMessageDelivery(OutboundMessageResult delivery) {
        String messageId = delivery.getOutboundMessage().getProperties().getMessageId();
        if (delivery.isReturned()) {
            returnedMessageCount.incrementAndGet();
            LOGGER.error("Message returned back {} ", messageId);
        }else {
            if (delivery.isAck()) {
                ackMessageCount.incrementAndGet();
                LOGGER.info("Sent successfully message {}", messageId);
            } else {
                nackMessageCount.incrementAndGet();
                LOGGER.error("Message nacked {}", messageId);
            }
        }
    }
    protected Flux<OutboundMessageResult> sendWithConfirmations(Flux<Integer> integers) {
        Flux<OutboundMessage> messageStream = integers.map(this::toAmqpMessage)
                .doOnRequest(n -> LOGGER.debug("Requesting {} messages", n))
                .delayElements(Duration.ofMillis(250))
                .doOnNext(m -> LOGGER.debug("Sending message {} ", m.getProperties().getMessageId()));

        return sender.sendWithPublishConfirms(messageStream, sendOptions);
    }
    @Override
    public String toString() {
        return String.format("Sent Summary: %d returned, %d confirmed, %d nacked |  %d sent\n",
                returnedMessageCount.get(),
                ackMessageCount.get(), nackMessageCount.get(), sentMessageCount.get());
    }
}

/**
 * When a message can be routed, the broker returns the message.
 *
 * What options do we have as a sender/producer ?
 *  - if we are in control of creating the queue then we can try bind it and retry
 *  - if we are not in control of queue/bindings then we either
 *      - don't use mandatory flag and rely on Alternate exchanges (outsource the solution)
 *      - send the message to another exchange-queue pair under our control (too much work, instead use AE)
 *      - report the problem (log, hospital-queue, fail back to originator if possible)
 *
 */
class ResilientIntegerSender_SendingUnroutableMessage extends ResilientIntegerSender {
    public ResilientIntegerSender_SendingUnroutableMessage(Sender sender, ResourceDeclaration resourceDeclaration, int count,  CountDownLatch countDownWhenTerminate) {
        super(sender, resourceDeclaration, count, countDownWhenTerminate);
    }
    @Override
    protected OutboundMessage toAmqpMessage(int index) {
        if ((index % 5) > 0) {
            // amq.direct exists but there should not be any bindings
            return new OutboundMessage("amq.direct", resourceDeclaration.queueName(),
                    withMessageId(index), withBody(index));
        }else {
            return super.toAmqpMessage(index);
        }
    }

}

/**
 * Uses plain basic.publish AMQP primitive. i.e. fire and forget.
 *
 * Only in case of IOExceptions (i.e. connectivity problems), the send operation throws an exception. The default exception handler
 * provided by SendOptions retries the operation.
 *
 * But what about other situations like returned or nacked messages ?
 *
 * The Mono returned from send() will complete after the flux of integers completes. All integers will be sent using the same
 * newly created Channel. It is ok if we dont overdo it otherwise we want to have a pool of channels and reuse them.
 *
 */
class IntegerSender implements CommandLineRunner {
    final Sender sender;
    final ResourceDeclaration resourceDeclaration;
    int count;
    static final Logger LOGGER = LoggerFactory.getLogger(IntegerSender.class);
    final SendOptions sendOptions;
    CountDownLatch countDownWhenTerminate;
    AtomicLong sentMessageCount = new AtomicLong();

    public IntegerSender(Sender sender, ResourceDeclaration resourceDeclaration, int count, CountDownLatch countDownWhenTerminate) {
        this.sender = sender;
        this.resourceDeclaration = resourceDeclaration;
        this.count = count;
        this.sendOptions = new SendOptions();
        this.countDownWhenTerminate =  countDownWhenTerminate;
    }

    public void run(String ... args) {
        resourceDeclaration.declare()
                .thenMany(send(integers(count)))
                .subscribe(d -> sentMessageCount.incrementAndGet(), this::handleError, countDownWhenTerminate::countDown);
    }

    protected Mono<Void> send(Flux<Integer> integers) {
        return sender.send(integers.map(this::toAmqpMessage), sendOptions);
    }

    protected Flux<Integer> integers(final int count) {
        return Flux.range(1, count);
    }

    protected OutboundMessage toAmqpMessage(int index) {

        return new OutboundMessage(resourceDeclaration.exchangeName(), resourceDeclaration.queueName(),
                withMessageId(index), withBody(index));
    }

    protected AMQP.BasicProperties withMessageId(int index) {
        return new AMQP.BasicProperties.Builder().messageId(String.format("%d-%d", System.nanoTime(), index)).build();
    }
    protected byte[] withBody(int index ) {
        return ("Message_" + index).getBytes();
    }
    protected void handleError(Throwable t) {
        LOGGER.error("Error occurred", t);
    }

    @Override
    public String toString() {
        return String.format("Sent Summary: %d sent", sentMessageCount);
    }

//    private Mono declareResourcesUsingAutoGeneratedQueue() {
//        return Mono.zip(
//                sender.declareExchange(myexchange()),
//                sender.declareQueue(QueueSpecification.queue()),
//                (e, q) -> bind(myexchange(), q));
//    }

}

class IntegerReceiver implements CommandLineRunner {
    private final Logger LOGGER = LoggerFactory.getLogger(IntegerReceiver.class);

    Receiver receiver;
    ResourceDeclaration resourceDeclaration;
    CountDownLatch countDownWhenTerminate;
    int expectedMessageCount;
    ConsumeOptions consumeOptions;

    public IntegerReceiver(Receiver receiver, ResourceDeclaration resourceDeclaration, int count, CountDownLatch countDownWhenTerminate) {
        this.receiver = receiver;
        this.resourceDeclaration = resourceDeclaration;
        this.countDownWhenTerminate = countDownWhenTerminate;
        this.expectedMessageCount = count;
        this.consumeOptions = new ConsumeOptions();

    }

    AtomicLong receivedMessageCount = new AtomicLong();

    public void run(String ... args ) {
        resourceDeclaration.declare()
                .thenMany(receiveIntegers())
                .doOnNext(this::processMessage)
                .take(expectedMessageCount) // We want the stream to complete when we have received all the messages
                .subscribe(this::log, this::handleError, countDownWhenTerminate::countDown);

    }
    protected void processMessage(Delivery delivery) {

    }
    protected void log(Delivery m) {
        long receivedSoFar = receivedMessageCount.incrementAndGet();
        LOGGER.info("Received message {}/{} [messageId:{}]", receivedSoFar, expectedMessageCount,
                m.getProperties().getMessageId());
    }
    protected Flux<Delivery> receiveIntegers() {
        return receiver.consumeNoAck(resourceDeclaration.queueName(), consumeOptions);
    }

    protected void handleError(Throwable t) {
        LOGGER.error("Error occurred", t);
    }

    @Override
    public String toString() {
        return String.format("Receiver Summary: %d/%d received", receivedMessageCount.get(), expectedMessageCount);
    }
}

class ResilientIntegerReceiverWithRandomExceptionWhileProcessingMessage extends ResilientIntegerReceiver {

    Random rand = new Random(System.currentTimeMillis());

    public ResilientIntegerReceiverWithRandomExceptionWhileProcessingMessage(Receiver receiver, ResourceDeclaration resourceDeclaration, int count, CountDownLatch countDownWhenTerminate) {
        super(receiver, resourceDeclaration, count, countDownWhenTerminate);
    }

    @Override
    protected void processMessage(Delivery d) {
        if (rand.nextBoolean()) {
            System.err.printf("Simulating exception on message %s\n", d.getProperties().getMessageId());
            throw new RuntimeException(String.format("Simulated exception processing message %s", d.getProperties().getMessageId()));
        }else {
            System.out.printf("Processing message %s\n", d.getProperties().getMessageId());
        }
    }
}
class ResilientIntegerReceiver extends IntegerReceiver {
    private final Logger LOGGER = LoggerFactory.getLogger(ResilientIntegerReceiver.class);

    MessageConsumerTemplate messageHandler = new MessageConsumerTemplate(this::processMessage);

    public ResilientIntegerReceiver(Receiver receiver, ResourceDeclaration resourceDeclaration, int count, CountDownLatch countDownWhenTerminate) {
        super(receiver, resourceDeclaration, count, countDownWhenTerminate);

        BiConsumer<Receiver.AcknowledgmentContext, Exception> exceptionHandler =
                new ExceptionHandlers.RetryAcknowledgmentExceptionHandler(
                        Duration.ofSeconds(20), Duration.ofMillis(500),
                        ExceptionHandlers.CONNECTION_RECOVERY_PREDICATE
                );
        consumeOptions.exceptionHandler(exceptionHandler);
    }

    public void run(String ... args ) {

        resourceDeclaration.declare()
                .thenMany(receiveAcknowledgableIntegers())
                .doOnNext(messageHandler)
                .take(expectedMessageCount) // We want the stream to complete when we have received all the messages
                .subscribe(this::log, this::handleError, countDownWhenTerminate::countDown);

    }

    protected Flux<AcknowledgableDelivery> receiveAcknowledgableIntegers() {
        return receiver.consumeManualAck(resourceDeclaration.queueName(), consumeOptions);
    }

    @Override
    public String toString() {
        return String.format("Receiver Summary: %d/%d received, %d nacked, %d acked",
                receivedMessageCount.get(), expectedMessageCount,
                messageHandler.nacked.get(), messageHandler.acked.get());
    }
}

class MessageConsumerTemplate implements Consumer<AcknowledgableDelivery> {

    private Consumer<AcknowledgableDelivery> consumer;

    public MessageConsumerTemplate(Consumer<AcknowledgableDelivery> consumer) {
        this.consumer = consumer;
    }

    AtomicLong nacked = new AtomicLong();
    AtomicLong acked = new AtomicLong();

    @Override
    public void accept(AcknowledgableDelivery d) {
        Throwable caught = null;
        try {
            consumer.accept(d);
        } catch(RuntimeException t) {
            caught = t;
        } catch(Exception t) {
            caught = t;
        }

        if (caught != null) {
            d.nack(false);
            nacked.incrementAndGet();
        }else {
            d.ack();
            acked.incrementAndGet();
        }
    }
}

/**
 * IMPORTANT CONCEPTS:
 * 1) ResourceManagement channel
 *
 *  By default, Sender creates a dedicated channel (via Mono) for resource declaration so that it does not interfeer
 * with publishing. This channel is cached. The Mono that provides the channel from a connection is created in the Sender
 * constructor and cached.
 *
 *
 * 2) Dedicated scheduler for resource management
 *
 *  Sender does also create a dedicated elastic scheduler called rabbitmq-sender-resource-creation. And it uses to
 *  process any declaration (<code>Mono<Channel>.publishOn(resourceScheduler)</code>
 *
 *  For instance, the logging statement "Resources are available" prints out a thread like `[....urce-creation-4]`
 *
 * 3) Caching
 *
 *  We want both, IntegerSEnder and IntegerReceiver, to use the same ResourceDeclaration instance. More specificaly, we want
 *  them to refer to the same Mono that declare the resources. The sender and the receiver's pipeline's start subscribing
 *  to the ResourceDeclaration's Mono. This means that , unless we cache it, it will declare teh resource twice. As far
 *  as RabbitMQ is concerned, this is not a problem. But it is a waste of cpu and network to do it.
 *
 *
 */
class ResourceDeclaration {
    private final Sender sender;
    private static final Logger LOGGER = LoggerFactory.getLogger(ResourceDeclaration.class);
    private final Mono<AMQP.Queue.BindOk> resourceDeclaration;

    public ResourceDeclaration(Sender sender) {
        this.sender = sender;
        this.resourceDeclaration = declareResources();
    }
    public String exchangeName() {
        return "integers";
    }
    public String queueName() {
        return "integers";
    }

    public Mono<AMQP.Queue.BindOk> declare() {
        return resourceDeclaration;
    }

    private Mono<AMQP.Queue.BindOk> declareResources() {
        return sender
                .declareExchange(ResourcesSpecification.exchange(exchangeName())).log()
                .then(sender.declareQueue(ResourcesSpecification.queue("integers")))
                .then(sender.bind(queueWithExchange()))
                .log()
                .doOnNext(bindOk -> {LOGGER.info("Resources are available");})
                .cache(); // comment out this line to see resources are declared twice. we don't want that.
    }
    private BindingSpecification queueWithExchange
            () {
        return ResourcesSpecification.binding("integers", queueName(), queueName());
    }
}
class ChaosMonkey {
    ReactorNettyClient http;

    ChaosMonkey(ReactorNettyClient http) {
        this.http = http;
    }

    void closeConnectionAfter(Mono<Connection> connection, Duration after) {
        connection.delaySubscription(after)
                .flatMapMany(conn -> http.getConnections()
                        .filter( connectionInfo -> conn.getClientProvidedName().equals(connectionInfo.getClientProperties().getConnectionName()))
                        .map(connectionInfo -> connectionInfo.getName())
                        .zipWith(connection.map(connection1 -> connection1.getClientProvidedName()))
                        .flatMap(tuple ->
                        {
                            System.err.printf("Closing %s connection %s \n", tuple.getT2(), tuple.getT1());
                            return http.closeConnection(tuple.getT1());
                        })
                ).subscribe();
    }
}
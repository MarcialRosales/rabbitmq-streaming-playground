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
import com.rabbitmq.client.Delivery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.rabbitmq.*;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
public class SpringBootSample {

    public static void main(String[] args) {
        SpringApplication.run(SpringBootSample.class, args).close();
    }

    @Value("${messageCount:10}")
    int messageCount;

    @Bean
    ResourceDeclaration resourceDeclaration(Sender sender) {
        return new ResourceDeclaration(sender);
    }
    //@Bean
    CommandLineRunner integerSender(Sender sender, ResourceDeclaration resourceDeclaration) {
        return new IntegerSender(sender, resourceDeclaration, messageCount);
    }
    //@Bean
    CommandLineRunner resilientIntegerSender(Sender sender, ResourceDeclaration resourceDeclaration) {
        return new ResilientIntegerSender(sender, resourceDeclaration, messageCount);
    }
    @Bean
    CommandLineRunner resilientIntegerSender_SendingUnroutableMessage(Sender sender, ResourceDeclaration resourceDeclaration) {
        return new ResilientIntegerSender_SendingUnroutableMessage(sender, resourceDeclaration, messageCount);
    }


    //@Bean
    CommandLineRunner integerSenderUsingUnknownExchange(Sender sender, ResourceDeclaration resourceDeclaration) {
        return new IntegerSenderUsingUnknownExchange(sender, resourceDeclaration, messageCount);
    }
    @Bean
    CommandLineRunner integerReceiver(Receiver receiver, ResourceDeclaration resourceDeclaration) {
        return new IntegerReceiver(receiver, resourceDeclaration, messageCount);
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

    public IntegerSenderUsingUnknownExchange(Sender sender, ResourceDeclaration resourceDeclaration, int count) {
        super(sender, resourceDeclaration, count);
    }

    @Override
    protected OutboundMessage toAmqpMessage(int index) {
        if (index > 2) {
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
    public ResilientIntegerSender(Sender sender, ResourceDeclaration resourceDeclaration, int count) {
        super(sender, resourceDeclaration, count);
        sendOptions.trackReturned(true);
    }
    public void run(String ... args) {
        resourceDeclaration.declare()
                .thenMany(sendWithConfirmations(integers(count)))
                .doOnError(System.err::println)
                .subscribe(m->{
                    if (m.isReturned()) {
                        System.err.println("Message returned back  "+new String(m.getOutboundMessage().getBody()));
                    }else {
                        if (m.isAck()) {
                            System.out.println("Sent successfully message " + new String(m.getOutboundMessage().getBody()));
                        } else {
                            System.err.println("Message nacked " + new String(m.getOutboundMessage().getBody()));
                        }
                    }
                });

        LOGGER.info("Finished sending integers");
    }
    protected Flux<OutboundMessageResult> sendWithConfirmations(Flux<Integer> integers) {
        return sender.sendWithPublishConfirms(integers.map(this::toAmqpMessage), sendOptions);
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
    public ResilientIntegerSender_SendingUnroutableMessage(Sender sender, ResourceDeclaration resourceDeclaration, int count) {
        super(sender, resourceDeclaration, count);
    }
    @Override
    protected OutboundMessage toAmqpMessage(int index) {
        if (index > 2) {
            // amq.direct exists but there should not be any bindings
            return new OutboundMessage("amq.direct", resourceDeclaration.queueName(), ("Message_" + index).getBytes());
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

    public IntegerSender(Sender sender, ResourceDeclaration resourceDeclaration, int count) {
        this.sender = sender;
        this.resourceDeclaration = resourceDeclaration;
        this.count = count;
        this.sendOptions = new SendOptions();
    }

    public void run(String ... args) {
        resourceDeclaration.declare()
                .then(send(integers(count)))
                .doOnError(System.err::println)
                .block();
        LOGGER.info("Finished sending integers");
    }

    protected Mono send(Flux<Integer> integers) {
        return sender.send(integers.map(this::toAmqpMessage), sendOptions);
    }

    protected Flux<Integer> integers(final int count) {
        return Flux.range(1, count);
    }

    protected OutboundMessage toAmqpMessage(int index) {
        return new OutboundMessage(resourceDeclaration.exchangeName(), resourceDeclaration.queueName(), ("Message_" + index).getBytes());
    }


//    private Mono declareResourcesUsingAutoGeneratedQueue() {
//        return Mono.zip(
//                sender.declareExchange(myexchange()),
//                sender.declareQueue(QueueSpecification.queue()),
//                (e, q) -> bind(myexchange(), q));
//    }

}
class IntegerReceiver implements CommandLineRunner {
    private Receiver receiver;
    private CountDownLatch allMessagesReceived;
    private ResourceDeclaration resourceDeclaration;
    private static final Logger LOGGER = LoggerFactory.getLogger(IntegerReceiver.class);

    public IntegerReceiver(Receiver receiver, ResourceDeclaration resourceDeclaration, int count) {
        this.receiver = receiver;
        this.resourceDeclaration = resourceDeclaration;
        this.allMessagesReceived = new CountDownLatch(count);
    }

    public void run(String ... args ) throws InterruptedException {
        resourceDeclaration.declare()
                .thenMany(receiveIntegers())
                //.take(allMessagesReceived.getCount())  we either use the take operator or call waitForAllMessages()
                .subscribe(m -> {
                    LOGGER.info("Received message {}", new String(m.getBody()));
                    allMessagesReceived.countDown();
                });
        waitForAllMessages();
    }
    private Flux<Delivery> receiveIntegers() {
        return receiver.consumeNoAck(resourceDeclaration.queueName());
    }
    public void waitForAllMessages() throws InterruptedException {
        allMessagesReceived.await(10, TimeUnit.SECONDS);
    }

}

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
                .declareExchange(ResourcesSpecification.exchange(exchangeName()))
                .then(sender.declareQueue(ResourcesSpecification.queue("integers")))
                .then(sender.bind(queueWithExchange()))
                .doOnNext(bindOk -> {LOGGER.info("Resources are available");})
                .cache(); // comment out this line to see resources are declared twice. we don't want that.
    }
    private BindingSpecification queueWithExchange
            () {
        return ResourcesSpecification.binding("integers", queueName(), queueName());
    }
}
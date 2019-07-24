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
    @Bean
    CommandLineRunner integerSender(Sender sender, ResourceDeclaration resourceDeclaration) {
        return new IntegerSender(sender, resourceDeclaration, messageCount);
    }
    @Bean
    CommandLineRunner integerReceiver(Receiver receiver, ResourceDeclaration resourceDeclaration) {
        return new IntegerReceiver(receiver, resourceDeclaration, messageCount);
    }

}


class IntegerSender implements CommandLineRunner {
    private final Sender sender;
    private final ResourceDeclaration resourceDeclaration;
    private int count;


    public IntegerSender(Sender sender, ResourceDeclaration resourceDeclaration, int count) {
        this.sender = sender;
        this.resourceDeclaration = resourceDeclaration;
        this.count = count;
    }
    public void run(String ... args) {
        resourceDeclaration.declareResources()
                .then(send(integers(count)))
                .subscribe();
    }

    private Mono send(Flux<Integer> integers) {
        return sender.send(integers.map(this::toAmqpMessage));
    }

    private Flux<Integer> integers(final int count) {
        return Flux.range(1, count);
    }
        private OutboundMessage toAmqpMessage(int index) {
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
        resourceDeclaration.declareResources()
                .thenMany(receiveIntegers())
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

    public ResourceDeclaration(Sender sender) {
        this.sender = sender;
    }
    public String exchangeName() {
        return "integers";
    }
    public String queueName() {
        return "integers";
    }

    public Mono<AMQP.Queue.BindOk> declareResources() {
        return sender
                .declareExchange(exchange())
                .then(sender.declareQueue(queue()))
                .then(sender.bind(queueWithExchange()));
    }
    private ExchangeSpecification exchange() {
        return ExchangeSpecification.exchange(exchangeName()); // default is direct
    }
    private QueueSpecification queue() {
        return QueueSpecification.queue("integers");
    }
    private BindingSpecification queueWithExchange
            () {
        return BindingSpecification.binding("integers", queueName(), queueName());
    }
}
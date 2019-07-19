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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.rabbitmq.OutboundMessage;
import reactor.rabbitmq.QueueSpecification;
import reactor.rabbitmq.Receiver;
import reactor.rabbitmq.Sender;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@SpringBootApplication
public class SpringBootSample {

    public static void main(String[] args) {
        SpringApplication.run(SpringBootSample.class, args).close();
    }

    @Bean
    CommandLineRunner runner(Sender sender, Receiver receiver) {
        return new Runner(sender, receiver, 10);
    }

}

class Runner implements CommandLineRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(SpringBootSample.class);

    private final Sender sender;
    private final Receiver receiver;
    private final int messageCount;
    CountDownLatch latch;

    Runner(Sender sender, Receiver receiver, int messageCount) {
        this.sender = sender;
        this.receiver = receiver;
        this.messageCount = messageCount;
        latch = new CountDownLatch(messageCount);
    }

    @Override
    public void run(String... args) throws Exception {
        Mono<AMQP.Queue.DeclareOk> queueDeclaration = sender.declareQueue(QueueSpecification.queue());
        queueDeclaration.subscribe(this::sendAndReceiveToFrom, this::reportError);
        latch.await(5, TimeUnit.SECONDS);
    }

    private void sendAndReceiveToFrom(AMQP.Queue.DeclareOk declaredQueue) {
        sendMessagesTo(declaredQueue);
        receiveAllMessagesFrom(declaredQueue);
    }
    private void reportError(Throwable error) {
        LOGGER.error("failed to declare queue", error);
    }

    private void receiveAllMessagesFrom(AMQP.Queue.DeclareOk queue) {
        receiver.consumeNoAck(queue.getQueue()).subscribe(m -> {
            LOGGER.info("Received message {}", new String(m.getBody()));
            latch.countDown();
        });
    }

    private void sendMessagesTo(AMQP.Queue.DeclareOk queue) {
        sender.send(
                Flux.range(1, messageCount).map(i -> toAmqpMessage(i, queue.getQueue()))
        ).subscribe();
    }

    private OutboundMessage toAmqpMessage(int index, String queue) {
        return new OutboundMessage("", queue, ("Message_" + index).getBytes());
    }
}
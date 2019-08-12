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

package com.example.partition;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import reactor.core.publisher.Flux;

import java.util.concurrent.CountDownLatch;

@SpringBootApplication
@Slf4j
public class PartitionProducerConsumerApplication {

    public static void main(String[] args) {
        SpringApplication.run(PartitionProducerConsumerApplication.class, args).close();
    }

    @Bean
    @ConditionalOnProperty(value = "role", havingValue = "producer", matchIfMissing = false)
    CommandLineRunner producer(RabbitMQConfiguration rabbit) {
        return (args) -> {
            Flux<Integer> integers = Flux.range(0, 1000);

            TopicsProperties topicDefinitions = new TopicsProperties();
            topicDefinitions.declare("topic1").withPartitions(2);

            TopicStream topic1 = rabbit.using(topicDefinitions).create("topic1");
            CountDownLatch done = new CountDownLatch(1);

            topic1.send(integers.map(String::valueOf))
                    .doOnTerminate(() -> done.countDown())
                    .subscribe();

            done.await();
            topic1.close();
        };
    }


    @Bean
    @ConditionalOnProperty(value = "role", havingValue = "consumer", matchIfMissing = false)
    CommandLineRunner consumer(RabbitMQConfiguration rabbit) {
        return (args) -> {
            TopicsProperties topicDefinitions = new TopicsProperties();
            topicDefinitions.declare("topic1").withPartitions(2);

            TopicStream topic1 = rabbit.using(topicDefinitions).create("topic1");

            CountDownLatch done = new CountDownLatch(1000);
            topic1.receive()
                    .subscribe(d -> {
                        done.countDown();
                        log.info("{}/{}",
                                d.getEnvelope().getRoutingKey(), done.getCount());
                    });

            done.await();
            topic1.close();
        };
    }

    @Bean
    @ConditionalOnProperty(value = "role", havingValue = "both", matchIfMissing = true)
    CommandLineRunner both(RabbitMQConfiguration rabbit) {
        return (args) -> {
            Flux<Integer> integers = Flux.range(0, 1000);

            TopicsProperties topicDefinitions = new TopicsProperties();
            topicDefinitions.declare("topic1").withPartitions(2);

            TopicStream topic1 = rabbit.using(topicDefinitions).create("topic1");

            topic1.send(integers.map(String::valueOf)).subscribe();

            CountDownLatch done = new CountDownLatch(1000);
            topic1.receive()
                    .subscribe(d -> {
                        done.countDown();
                        log.info("{}/{}",
                                d.getEnvelope().getRoutingKey(), done.getCount());
                    });

            done.await();
            topic1.close();
        };
    }
}


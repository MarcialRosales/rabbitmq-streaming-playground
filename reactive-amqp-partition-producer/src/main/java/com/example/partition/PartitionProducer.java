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

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import reactor.core.publisher.Flux;

@SpringBootApplication
public class PartitionProducer {

    public static void main(String[] args) {
        SpringApplication.run(PartitionProducer.class, args).close();
    }

    @Bean
    CommandLineRunner runner(RabbitMQConfiguration rabbit) {
        return (args) -> {
            Flux<Integer> integers = Flux.range(0, 1000);

            TopicsProperties topicDefinitions = new TopicsProperties();
            topicDefinitions.declare("topic1").withPartitions(2);
            topicDefinitions.declare("topic2");

            rabbit.using(topicDefinitions)
                    .createTopic("topic1")
                    .send(integers.map(String::valueOf), true)
                    .subscribe();

            // NOTE: give enough time to flush all messages over the network
            // else the program finishes while still sending messages
            // sending with confirms would allow us to get a confirmation when the message is sent
            // and used that to wait rather than a random delay
            Thread.sleep(5000);

        };
    }

}


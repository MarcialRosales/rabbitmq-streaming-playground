package com.example.partition;

import com.rabbitmq.http.client.Client;
import com.rabbitmq.http.client.HttpResponse;
import com.rabbitmq.http.client.ReactorNettyClient;
import com.rabbitmq.http.client.domain.PolicyInfo;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.rabbitmq.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;


@Slf4j
public class TopicStreamBuilder {

    private TopicsProperties topics;
    private RabbitMQConfiguration config;

    public TopicStreamBuilder(TopicsProperties topics, RabbitMQConfiguration config) {
        this.topics = topics;
        this.config = config;
    }

    public TopicStream create(String topic) {
        if (topics.contains(topic)) {
            return new TopicStream(topics.get(topic), config.createSender(), config.createReceiver(),
                    new TopicConfigurer(config.getVirtualHost(), config.rabbitAdminClient()));
        }else {
            throw new NoSuchElementException(topic);
        }
    }

}

@Slf4j
class TopicConfigurer {
    private String vhost;
    private ReactorNettyClient rabbitAdmin;

    public TopicConfigurer(String vhost, ReactorNettyClient rabbitAdmin) {
        this.vhost = vhost;
        this.rabbitAdmin = rabbitAdmin;
    }

    public Flux<String> declare(Topic topic, Sender sender) {
        return declarePolicyForTopic(topic).doOnSuccess(c -> log.info("Successfully declared policy {}", topic.name))
                .then(declareExchange(sender, topic).doOnNext(c -> log.info("Declared exchange")))
                .thenMany(topic.partitionQueues())
                .concatMap(queue -> declareQueue(sender, queue).then(bind(sender, topic, queue)))
                .thenMany(topic.partitionQueues());
    }

    private Mono<HttpResponse> declarePolicyForTopic(Topic topic) {
        return rabbitAdmin.declarePolicy(vhost == null ? "/" : vhost, topic.name, declarePolicy(topic));
    }
    private PolicyInfo declarePolicy(Topic topic) {
        PolicyInfo policy = new PolicyInfo();
        policy.setName(topic.name);
        policy.setApplyTo("queues"); // we use a single policy for exchange and queues
        policy.setPattern(String.format("^%s", topic.name));
        policy.setDefinition(
                new PolicyDefinitionBuilder()
                        .minMastersQueueMasterLocator()
                        .maxLength(1500)
                        .build());
        return policy;
    }

    private Mono<?> declareExchange(Sender sender, Topic topic) {
        return sender.
                declareExchange(ResourcesSpecification.exchange(
                        topic.getName())
                        .durable(true)
                        .type("x-consistent-hash")
                        .arguments(partitionKey(topic)));
    }
    public String getPartitionKeyValue(String v) {
        return String.valueOf(v.hashCode());    // note: hardcoded for now
    }
    private static Map<String, Object> partitionKey(Topic topic) {
        switch(topic.partitionKey) {
            case "routingKey": return null;
            default:
                throw new UnsupportedOperationException("not supported yet");
        }
    }
    private static Map<String, Object> singleActiveConsumer() {
        return  mapOf("x-single-active-consumer", true);
    }


    private static Map<String, Object> mapOf(String key, Object value) {
        Map<String, Object> map = new HashMap<>(1);
        map.put(key, value);
        return Collections.unmodifiableMap(map);
    }

    protected Mono<?> declareQueue(Sender sender, String queue) {
        return sender
                .declareQueue(
                        ResourcesSpecification.queue(queue).durable(true).arguments(singleActiveConsumer()));
    }

    protected Mono<?> bind(Sender sender, Topic topic, String queue) {
        return sender.bind(ResourcesSpecification.binding(
                topic.getName(), "1", queue));
    }

}

class PolicyDefinitionBuilder {
    Map<String, Object> definitions = new HashMap<>();

    public PolicyDefinitionBuilder minMastersQueueMasterLocator() {
        definitions.put("queue-master-locator", "min-masters");
        return this;
    }
    public PolicyDefinitionBuilder maxLength(int length) {
        return maxLength(length, null);
    }
    public PolicyDefinitionBuilder maxLength(int length, String strategy) {
        definitions.put("max-length", length);
        definitions.put("overflow", strategy == null ? "reject-publish" : strategy);
        return this;
    }
    public Map<String, Object> build() {
        return definitions;
    }

}

class TopicStream {
    Topic topic;
    Sender sender;
    Receiver receiver;
    TopicConfigurer configurer;

    public TopicStream(Topic topic, Sender sender, Receiver receiver, TopicConfigurer configurer) {
        this.topic = topic;
        this.sender = sender;
        this.receiver = receiver;
        this.configurer = configurer;
    }

    public Flux<com.rabbitmq.client.Delivery> receive() {
        return configurer
                .declare(topic, sender)
                .flatMap(receiver::consumeAutoAck);
    }
    public Mono<Void> send(Flux<String> stream) {
        return configurer
                .declare(topic, sender)
                .then(sendStream(stream));
    }
    public void close() {
        sender.close();
        receiver.close();
    }

    protected Mono<Void> sendStream(Flux<String> stream) {
        return sender.send(stream.map(v -> new OutboundMessage(topic.getName(), configurer.getPartitionKeyValue(v),
                v.getBytes())));
    }

}

@Data
class TopicsProperties {
    private Map<String, Topic> topics = new HashMap<>();

    public Topic get(String name) {
        return topics.get(name);
    }
    public boolean contains(String name) {
        return topics.containsKey(name);
    }
    Topic declare(String name) {
        Topic topic = new Topic(name);
        topics.put(topic.name, topic);
        return topic;
    }

}

@Data
@AllArgsConstructor
class Topic {

    @NonNull
    String name;

    String queuePrefix = "-q";

    int partitions = 1;

    String partitionKey = "routingKey";

    public Topic(@NonNull String name, int partitions) {
        this(name);
        this.partitions = partitions;
    }

    public Topic(String name) {
        this.name = name;
    }


    public Topic withPartitions(int count) {
        this.partitions = count;
        return this;
    }
    public Topic withPartitionKey(String key) {
        // TODO validate key
        // hash-property:correlation_id
        // hash-property:message_id
        // hash-header:<headerName>
        // routingKey
        this.partitionKey = key;
        return this;
    }
    private String getPartitionQueueName(int index) {
        return String.format("%s%s-%d", name,  queuePrefix, index);
    }
    public Flux<String> partitionQueues() {
        return Flux.range(1, partitions)
                .map(this::getPartitionQueueName);

    }


}
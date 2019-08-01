package com.example.partition;

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
public class ResourceDeclaration {

    TopicsProperties topics;
    RabbitMQConfiguration rabbit;

    public ResourceDeclaration(TopicsProperties topics, RabbitMQConfiguration rabbit) {
        this.topics = topics;
        this.rabbit = rabbit;
    }

    public TopicStream createTopic(String topic) {
        if (topics.contains(topic)) {
            return new TopicStream(topics.get(topic), rabbit.createSender(), rabbit.createReceiver());
        }else {
            throw new NoSuchElementException(topic);
        }
    }


}

class TopicStream {
    Topic topic;
    Sender sender;
    Receiver receiver;

    public TopicStream(Topic topic, Sender sender, Receiver receiver) {
        this.topic = topic;
        this.sender = sender;
        this.receiver = receiver;
    }

    public Flux<com.rabbitmq.client.Delivery> receive() {
        return declareTopic(topic)
                .thenMany(topic.partitionQueues())
                .flatMap(receiver::consumeAutoAck);
    }
    public Mono<Void> send(Flux<String> stream) {
        return declareTopic(topic)
                .then(sendStream(stream));
    }
    public void close() {
        sender.close();
        receiver.close();
    }


    protected Mono<Void> sendStream(Flux<String> stream) {
        return sender.send(stream.map(v -> new OutboundMessage(topic.getName(), getPartitionKeyValue(v),
                v.getBytes())));
    }

    protected Flux<?> declareTopic(Topic topic) {
        return declareExchange(topic)
                .thenMany(topic.partitionQueues())
                .concatMap(queue -> declareQueue(queue).then(bind(topic, queue)));
    }
    protected Mono<?> declareExchange(Topic topic) {
        return sender.
                declareExchange(ResourcesSpecification.exchange(
                        topic.getName())
                        .durable(true)
                        .type("x-consistent-hash")
                        .arguments(partitionKey(topic)));
    }
    protected String getPartitionKeyValue(String v) {
        return String.valueOf(v.hashCode());    // note: hardcoded for now
    }
    protected static Map<String, Object> partitionKey(Topic topic) {
        switch(topic.partitionKey) {
            case "routingKey": return null;
            default:
                throw new UnsupportedOperationException("not supported yet");
        }
    }
    protected static Map<String, Object> singleActiveConsumer() {
        return  mapOf("x-single-active-consumer", true);
    }


    protected static Map<String, Object> mapOf(String key, Object value) {
        Map<String, Object> map = new HashMap<>(1);
        map.put(key, value);
        return Collections.unmodifiableMap(map);
    }

    protected Mono<?> declareQueue(String queue) {
        return sender
                .declareQueue(
                        ResourcesSpecification.queue(queue).durable(true).arguments(singleActiveConsumer()));
    }

    protected Mono<?> bind(Topic topic, String queue) {
        return sender.bind(ResourcesSpecification.binding(
                topic.getName(), "1", queue));
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
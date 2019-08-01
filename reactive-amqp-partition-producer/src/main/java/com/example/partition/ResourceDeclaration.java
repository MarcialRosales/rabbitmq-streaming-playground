package com.example.partition;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.rabbitmq.OutboundMessage;
import reactor.rabbitmq.ResourcesSpecification;
import reactor.rabbitmq.Sender;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;


@Slf4j
public class ResourceDeclaration {

    TopicsProperties topics;
    Sender sender;

    public ResourceDeclaration(TopicsProperties topics, Sender sender) {
        this.sender = sender;
        this.topics = topics;
    }

    public TopicSender createTopic(String topic) {
        if (topics.contains(topic)) {
            return new TopicSender(sender, topics.get(topic));
        }else {
            throw new NoSuchElementException(topic);
        }
    }

//    public Mono<?> declareTopic(String ... names) {
//        return Flux.just(names)
//                .filter(topics::contains)
//                .map(topics::get)
//                .doOnNext(topic -> log.info("Declaring topic {}", topic))
//                .concatMap(topic -> declareTopic(topic))
//                .then(Mono.empty());
//    }
}
class TopicSender {
    Sender sender;
    Topic topic;

    public TopicSender(Sender sender, Topic topic) {
        this.sender = sender;
        this.topic = topic;
    }
    public void close() {
        sender.close();
    }
    public Mono<Void> send(Flux<String> stream, boolean closeWhenDone) {
        return declareTopic(topic)
                .then(sendStream(stream)).doOnTerminate(() -> { if (closeWhenDone) sender.close(); });
    }

    private Mono<Void> sendStream(Flux<String> stream) {
        return sender.send(stream.map(v -> new OutboundMessage(topic.getName(), getPartitionKeyValue(v),
                v.getBytes())));
    }

    private Flux<?> declareTopic(Topic topic) {
        return declareExchange(topic)
                .thenMany(Flux.range(1, topic.partitions))
                .map(index -> partitionQueue(topic, index))
                .concatMap(queue -> declareQueue(queue).then(bind(topic, queue)));
    }
    private Mono<?> declareExchange(Topic topic) {
        return sender.
                declareExchange(ResourcesSpecification.exchange(
                        topic.getName())
                        .durable(true)
                        .type("x-consistent-hash")
                        .arguments(partitionKey(topic)));
    }
    private String getPartitionKeyValue(String v) {
        return String.valueOf(v.hashCode());    // note: hardcoded for now
    }
    private static Map<String, Object> partitionKey(Topic topic) {
        switch(topic.partitionKey) {
            case "routingKey": return null;
            default:
                throw new UnsupportedOperationException("not supported yet");
        }
    }
    private static Map<String, Object> mapOf(String key, String value) {
        Map<String, Object> map = new HashMap<>(1);
        map.put(key, value);
        return Collections.unmodifiableMap(map);
    }

    private Mono<?> declareQueue(String queue) {
        return sender
                .declareQueue(
                        ResourcesSpecification.queue(queue).durable(true));
    }
    private String partitionQueue(Topic topic, int index) {
        return String.format("%s%s-%d", topic.getName(), topic.getQueuePrefix(), index);
    }
    private Mono<?> bind(Topic topic, String queue) {
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
}
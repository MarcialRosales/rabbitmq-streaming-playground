package com.example.wordcount;

import reactor.core.publisher.Mono;
import reactor.rabbitmq.*;
import reactor.util.function.Tuple2;

import java.util.concurrent.atomic.LongAdder;

public class WordCountResources {

    String wordCountInputExchange = "wordcount-input-x";
    String wordCountInputQueue = "wordcount-input-q";
    String wordCountInputRoutingKey = "line";

    String wordCountOutputQueue = "wordcount-output-q";
    String wordCountOutputExchange = "wordcount-output-x";
    String wordCountOutputRoutingKey = "wordcount";

    private Sender sender;

    public WordCountResources(Sender sender) {
        this.sender = sender;
    }

    public Mono<?> declare() {
        return
                sender.declareQueue(wordCountOutputQueue())
                .then(sender.declareQueue(wordCountInputQueue()))
                .then(sender.declareExchange(wordCountInputExchange()))
                .then(sender.declareExchange(wordCountOutputExchange()))
                .then(sender.bind(wordCountInput()))
                .then(sender.bind(wordCountOutput()));

    }

    private BindingSpecification wordCountInput() {
        return ResourcesSpecification.binding(wordCountInputExchange, wordCountInputRoutingKey,
                wordCountInputQueue);
    }
    private BindingSpecification wordCountOutput() {
        return ResourcesSpecification.binding(wordCountOutputExchange, wordCountOutputRoutingKey,
                wordCountOutputQueue);
    }
    private QueueSpecification wordCountOutputQueue() {
        return ResourcesSpecification.queue(wordCountOutputQueue).durable(true);
    }
    private QueueSpecification wordCountInputQueue() {
        return ResourcesSpecification.queue(wordCountInputQueue).durable(true);
    }
    private ExchangeSpecification wordCountInputExchange() {
        return ResourcesSpecification.exchange(wordCountInputExchange).durable(true);
    }
    private ExchangeSpecification wordCountOutputExchange() {
        return ResourcesSpecification.exchange(wordCountOutputExchange).durable(true);
    }
    public OutboundMessage messageToWordCountQueue(Tuple2<String, LongAdder> wordCount) {
        return messageTo(wordCount, wordCountOutputRoutingKey);
    }

    public OutboundMessage messageTo(Tuple2<String, LongAdder> wordCount, String routingKey) {
        return new OutboundMessage(wordCountOutputExchange, routingKey,
                String.format("%s:%d", wordCount.getT1(), wordCount.getT2().longValue()).getBytes());
    }
}

package com.example.demo;

import reactor.core.publisher.Mono;
import reactor.rabbitmq.ExchangeSpecification;
import reactor.rabbitmq.QueueSpecification;
import reactor.rabbitmq.ResourcesSpecification;
import reactor.rabbitmq.Sender;

public class WordCountResources {

    String wordCountInputExchange = "wordcount-input-x";
    String wordCountInputQueue = "wordcount-input-q";

    String wordCountOutputQueue = "wordcount-output-q";

    private Sender sender;

    public WordCountResources(Sender sender) {
        this.sender = sender;
    }

    public Mono<?> declare() {
        return
                sender.declareQueue(wordCountOutputQueue())
                .then(sender.declareQueue(wordCountInputQueue()))
                .then(sender.declareExchange(wordCountInputExchange()))
                .then(sender.bind(ResourcesSpecification.binding(wordCountInputExchange, wordCountInputQueue,
                        wordCountInputQueue)));

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

}

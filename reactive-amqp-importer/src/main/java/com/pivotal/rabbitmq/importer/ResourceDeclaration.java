package com.pivotal.rabbitmq.importer;

import reactor.core.publisher.Mono;
import reactor.rabbitmq.ResourcesSpecification;
import reactor.rabbitmq.Sender;

class ResourceDeclaration {
    Sender sender;
    FileImporterConfigurationProperties properties;

    public ResourceDeclaration(Sender sender, FileImporterConfigurationProperties properties) {
        this.sender = sender;
        this.properties = properties;
    }
    public Mono<?> declare() {
        return declareExchangeIfAny()
                .then(declareQueueIfAny())
                .then(declareBindingIfAny());
    }
    private Mono<?> declareBindingIfAny() {
        if (properties.getRoutingKey() != null && properties.exchange != null &&
            !properties.exchange.isEmpty() && properties.queue != null) {
            return sender.bind(ResourcesSpecification.binding(properties.exchange, properties.getRoutingKey(),
                    properties.queue));
        }else {
            return Mono.empty();
        }
    }
    private Mono<?> declareQueueIfAny() {
        if (properties.queue != null) {
            return sender.declareQueue(ResourcesSpecification.queue(properties.queue)
                    .durable(properties.durableQueue));
        }else {
            return Mono.empty();
        }

    }
    private Mono<?> declareExchangeIfAny() {
        if (properties.exchange != null && !properties.exchange.isEmpty()) {
            return sender.declareExchange(ResourcesSpecification.exchange(properties.exchange)
                    .type(properties.exchangeType));
        }else {
            return Mono.empty();
        }
    }
}

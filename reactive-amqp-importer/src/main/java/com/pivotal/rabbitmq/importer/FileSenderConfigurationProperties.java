package com.pivotal.rabbitmq.importer;

import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.net.URI;

@ConfigurationProperties("file-sender")
@Data
class FileSenderConfigurationProperties {
    String queue;
    boolean durableQueue;

    String exchange = "";
    String exchangeType = "direct";

    private @Getter(AccessLevel.NONE) String routingKey;
    public String getRoutingKey() {
        return routingKey != null ? routingKey : queue;
    }

    private @Getter(AccessLevel.NONE) String uri;
    public URI getUri() { return uri != null ? URI.create(uri) : null; }

    String skipLinesStartWith;

    boolean shouldSkipLine(String line) {
        return skipLinesStartWith == null || !line.startsWith(skipLinesStartWith);
    }

}

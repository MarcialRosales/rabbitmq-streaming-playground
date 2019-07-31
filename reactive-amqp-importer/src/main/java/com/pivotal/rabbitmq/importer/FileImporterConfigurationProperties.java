package com.pivotal.rabbitmq.importer;

import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.net.URI;

@ConfigurationProperties()
@Data
class FileImporterConfigurationProperties {
    String queue;
    boolean durableQueue = true;

    String exchange = "";
    String exchangeType = "direct";

    @Getter(AccessLevel.NONE) String routingKey;
    public String getRoutingKey() {
        return routingKey != null ? routingKey : queue;
    }

    @Getter(AccessLevel.NONE) String uri;
    public URI getUri() { return uri != null ? URI.create(uri) : null; }

    String skipLinesStartWith;

    boolean shouldSkipLine(String line) {
        return skipLinesStartWith == null || !line.startsWith(skipLinesStartWith);
    }

}

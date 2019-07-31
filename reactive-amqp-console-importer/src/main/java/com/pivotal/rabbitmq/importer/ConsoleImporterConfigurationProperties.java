package com.pivotal.rabbitmq.importer;

import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.net.URI;

@ConfigurationProperties()
@Data
class ConsoleImporterConfigurationProperties {
    String queue;
    boolean durableQueue = true;

    String exchange = "";
    String exchangeType = "direct";

    @Getter(AccessLevel.NONE) String routingKey;
    public String getRoutingKey() {
        return routingKey != null ? routingKey : queue;
    }

    String skipLinesStartWith;

    boolean shouldSkipLine(String line) {
        return skipLinesStartWith == null || !line.startsWith(skipLinesStartWith);
    }

}

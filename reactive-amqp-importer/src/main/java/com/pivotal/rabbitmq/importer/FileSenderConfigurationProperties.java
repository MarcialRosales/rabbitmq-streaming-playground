package com.pivotal.rabbitmq.importer;

import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("file-sender")
@Data
class FileSenderConfigurationProperties {
    String queue;
    boolean durableQueue;

    String exchange = "";
    String exchangeType = "direct";

    private @Getter(AccessLevel.NONE) String routingKey;

    String filename;
    String skipLinesStartWith;

    boolean deliveryGuaranteed = true;

    boolean shouldSkipLine(String line) {
        return skipLinesStartWith == null || !line.startsWith(skipLinesStartWith);
    }
    public String getRoutingKey() {
        return routingKey != null ? routingKey : queue;
    }
}

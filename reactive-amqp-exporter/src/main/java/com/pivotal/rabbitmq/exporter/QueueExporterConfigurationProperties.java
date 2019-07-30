package com.pivotal.rabbitmq.exporter;

import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("queue-exporter")
@Data
class QueueExporterConfigurationProperties {
    String queue;
    int qos = 250;

    private @Getter(AccessLevel.NONE) long consumeTimeout = DEFAULT_CONSUME_TIMEOUT_MS;
    public static final long DEFAULT_CONSUME_TIMEOUT_MS = 250;

    public long getConsumeTimeout() {
        return Math.max(consumeTimeout, DEFAULT_CONSUME_TIMEOUT_MS);
    }

}

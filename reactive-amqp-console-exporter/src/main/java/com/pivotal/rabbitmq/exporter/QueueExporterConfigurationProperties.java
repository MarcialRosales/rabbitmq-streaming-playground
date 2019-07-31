package com.pivotal.rabbitmq.exporter;

import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties()
@Data
class QueueExporterConfigurationProperties {
    String queue;

    int batchSize = 250;

    private @Getter(AccessLevel.NONE) int qosMultiplerOfBatchSize = 2;

    public int getQosMultiplerOfBatchSize() {
        return Math.max(1, qosMultiplerOfBatchSize);
    }
    private @Getter(AccessLevel.NONE) long consumeTimeout = DEFAULT_CONSUME_TIMEOUT_MS;
    public static final long DEFAULT_CONSUME_TIMEOUT_MS = 250;

    public long getConsumeTimeout() {
        return Math.max(consumeTimeout, DEFAULT_CONSUME_TIMEOUT_MS);
    }

    public int getQos() {
        return batchSize * qosMultiplerOfBatchSize;
    }

    long idleTimeSec = -1;

}

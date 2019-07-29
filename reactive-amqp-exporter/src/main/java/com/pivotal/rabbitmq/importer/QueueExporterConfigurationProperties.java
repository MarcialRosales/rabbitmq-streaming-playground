package com.pivotal.rabbitmq.importer;

import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.net.URI;

@ConfigurationProperties("queue-exporter")
@Data
class QueueExporterConfigurationProperties {
    String queue;
    boolean durableQueue;

    private @Getter(AccessLevel.NONE) String filenamePrefix;


}

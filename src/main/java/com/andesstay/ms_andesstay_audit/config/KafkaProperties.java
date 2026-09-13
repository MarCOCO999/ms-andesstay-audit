package com.andesstay.ms_andesstay_audit.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "andesstay.kafka")
public record KafkaProperties(
        String topicReservationsEvents,
        String topicAuditTimeline,
        int partitions,
        int replicationFactor
) {
}

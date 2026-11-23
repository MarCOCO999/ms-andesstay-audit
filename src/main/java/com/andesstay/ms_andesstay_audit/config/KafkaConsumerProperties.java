package com.andesstay.ms_andesstay_audit.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** Politica de reintentos de los consumidores Kafka (andesstay.kafka.consumer en application.yml). */
@ConfigurationProperties(prefix = "andesstay.kafka.consumer")
public record KafkaConsumerProperties(Retry retry) {

    /**
     * @param maxRetries        reintentos despues del primer intento (3 = 4 intentos en total)
     * @param initialIntervalMs espera antes del primer reintento
     * @param multiplier        factor por el que crece la espera en cada reintento
     * @param maxIntervalMs     tope de la espera entre reintentos
     */
    public record Retry(int maxRetries, long initialIntervalMs, double multiplier, long maxIntervalMs) {
    }
}

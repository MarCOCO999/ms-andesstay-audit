package com.andesstay.ms_andesstay_audit.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Topicos con los que trabaja audit (andesstay.kafka.topics en application.yml).
 *
 * @param reservationsEvents    nombre del topico que consume (lo declara ms-andesstay-reservations)
 * @param reservationsEventsDlt DLT de reservations.events, al que este servicio envia lo que no pudo procesar
 * @param auditTimeline         topico que este servicio produce
 */
@ConfigurationProperties(prefix = "andesstay.kafka.topics")
public record KafkaTopicsProperties(String reservationsEvents, TopicSpec reservationsEventsDlt, TopicSpec auditTimeline) {
}

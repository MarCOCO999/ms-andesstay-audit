package com.andesstay.ms_andesstay_audit.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Topicos que audit escribe (cada topico lo declara quien lo produce): audit.timeline, el
 * historial normalizado que republica tras persistir cada evento, y reservations.events.DLT,
 * donde deja lo que no pudo procesar. reservations.events lo declara ms-andesstay-reservations.
 * Al arrancar, el KafkaAdmin de Spring los crea si no existen y, con
 * spring.kafka.admin.modify-topic-configs, reaplica su configuracion.
 */
@Configuration
public class KafkaTopicsConfig {

    @Bean
    public NewTopic auditTimelineTopic(KafkaTopicsProperties topics) {
        return topics.auditTimeline().toNewTopic();
    }

    @Bean
    public NewTopic reservationsEventsDeadLetterTopic(KafkaTopicsProperties topics) {
        return topics.reservationsEventsDlt().toNewTopic();
    }
}

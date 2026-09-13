package com.andesstay.ms_andesstay_audit.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Topicos del caso AndesStay (seccion 9). reservations.events es la fuente de verdad
 * publicada por ms-andesstay-reservations; audit.timeline es el historial normalizado
 * que este servicio republica tras persistir cada evento. El *.DLT de reservations.events
 * lo crea automaticamente el DeadLetterPublishingRecoverer (ver KafkaConsumerConfig) la
 * primera vez que un mensaje agota sus reintentos, pero declararlo aqui asegura que ya
 * exista con la configuracion correcta desde el arranque.
 */
@Configuration
public class KafkaTopicsConfig {

    private final KafkaProperties kafkaProperties;

    public KafkaTopicsConfig(KafkaProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
    }

    @Bean
    public NewTopic reservationsEventsTopic() {
        return topic(kafkaProperties.topicReservationsEvents());
    }

    @Bean
    public NewTopic reservationsEventsDeadLetterTopic() {
        return topic(kafkaProperties.topicReservationsEvents() + ".DLT");
    }

    @Bean
    public NewTopic auditTimelineTopic() {
        return topic(kafkaProperties.topicAuditTimeline());
    }

    private NewTopic topic(String name) {
        return new NewTopic(name, kafkaProperties.partitions(), (short) kafkaProperties.replicationFactor());
    }
}

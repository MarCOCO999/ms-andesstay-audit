package com.andesstay.ms_andesstay_audit.messaging;

import com.andesstay.ms_andesstay_audit.config.KafkaProperties;
import com.andesstay.ms_andesstay_audit.domain.AuditEvent;
import com.andesstay.ms_andesstay_audit.repository.AuditEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class ReservationEventListener {

    private static final Logger log = LoggerFactory.getLogger(ReservationEventListener.class);

    private final AuditEventRepository repository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final KafkaProperties kafkaProperties;

    public ReservationEventListener(AuditEventRepository repository,
                                     KafkaTemplate<String, Object> kafkaTemplate,
                                     KafkaProperties kafkaProperties) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaProperties = kafkaProperties;
    }

    @KafkaListener(topics = "${andesstay.kafka.topic-reservations-events}")
    public void onReservationEvent(ReservationEvent event) {
        if (repository.existsByEventId(event.eventId())) {
            log.info("Evento {} ya estaba auditado, se ignora el redelivery", event.eventId());
            return;
        }

        AuditEvent auditEvent = AuditEvent.builder()
                .eventId(event.eventId())
                .eventType(event.type())
                .reservationId(event.reservationId())
                .previousStatus(event.previousStatus())
                .newStatus(event.newStatus())
                .actorId(event.actorId())
                .actorRole(event.actorRole())
                .unitId(event.unitId())
                .occurredAt(event.timestamp())
                .build();
        repository.save(auditEvent);

        kafkaTemplate.send(kafkaProperties.topicAuditTimeline(), event.reservationId(), AuditTimelineEntry.from(event));
        log.info("Reserva {}: {} -> {} (evento {})", event.reservationId(), event.previousStatus(), event.newStatus(), event.eventId());
    }
}

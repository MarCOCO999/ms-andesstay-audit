package com.andesstay.ms_andesstay_audit.messaging;

import com.andesstay.ms_andesstay_audit.domain.AuditEvent;
import com.andesstay.ms_andesstay_audit.service.AuditRecordingService;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * Adaptador de mensajeria: recibe y valida el evento de reservations.events, lo traduce a la
 * entidad de dominio y delega en {@link AuditRecordingService}; despues publica la entrada
 * normalizada en audit.timeline. No tiene logica de negocio.
 * <p>
 * Consumer group "ms-andesstay-audit": audit recibe TODOS los eventos, independiente de report
 * (que tiene su propio grupo). Con concurrency=3 hay un consumidor por particion; si se levantan
 * mas instancias de audit, comparten el grupo y Kafka reparte las particiones entre ellas.
 */
@Component
public class ReservationEventListener {

    private static final Logger log = LoggerFactory.getLogger(ReservationEventListener.class);

    private final AuditRecordingService auditRecordingService;
    private final AuditTimelinePublisher timelinePublisher;

    public ReservationEventListener(AuditRecordingService auditRecordingService, AuditTimelinePublisher timelinePublisher) {
        this.auditRecordingService = auditRecordingService;
        this.timelinePublisher = timelinePublisher;
    }

    @KafkaListener(
            id = "audit-reservations-events",
            topics = "${andesstay.kafka.topics.reservations-events}",
            groupId = "${andesstay.kafka.consumer-groups.reservations-events}",
            concurrency = "${andesstay.kafka.consumer.concurrency}")
    public void onReservationEvent(@Payload @Valid ReservationEvent event,
                                   @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                                   @Header(KafkaHeaders.OFFSET) long offset) {
        log.debug("Recibido {} de la reserva {} (particion {}, offset {})", event.type(), event.reservationId(), partition, offset);

        AuditEvent recorded = auditRecordingService.record(toAuditEvent(event));
        timelinePublisher.publish(recorded);
    }

    private static AuditEvent toAuditEvent(ReservationEvent event) {
        return AuditEvent.builder()
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
    }
}

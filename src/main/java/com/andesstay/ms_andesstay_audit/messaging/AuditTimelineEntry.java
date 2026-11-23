package com.andesstay.ms_andesstay_audit.messaging;

import com.andesstay.ms_andesstay_audit.domain.AuditEvent;

import java.time.Instant;

/**
 * Version normalizada que este servicio publica en audit.timeline (key = reservationId) despues
 * de persistir el evento crudo en Oracle: "quien / que / cuando" del ultimo cambio de cada
 * reserva. El topico es compactado, asi que Kafka conserva al menos el ultimo registro de cada
 * reserva: ms-andesstay-report lo consume para mantener el estado actual de todas las reservas
 * sin consultar la base de datos de reservations.
 */
public record AuditTimelineEntry(
        String eventId,
        String reservationId,
        String eventType,
        String previousStatus,
        String newStatus,
        String actorId,
        String actorRole,
        Long unitId,
        Instant occurredAt
) {
    public static AuditTimelineEntry from(AuditEvent event) {
        return new AuditTimelineEntry(
                event.getEventId(), event.getReservationId(), event.getEventType(), event.getPreviousStatus(),
                event.getNewStatus(), event.getActorId(), event.getActorRole(), event.getUnitId(), event.getOccurredAt());
    }
}

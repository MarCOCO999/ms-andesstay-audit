package com.andesstay.ms_andesstay_audit.messaging;

import java.time.Instant;

/**
 * Version normalizada que este servicio republica en audit.timeline (topico compactado
 * por reservationId) despues de persistir el evento crudo en Oracle. Sirve como historial
 * "quien / que / cuando / desde donde" listo para otros consumidores (p.ej. un dashboard).
 */
public record AuditTimelineEntry(
        String reservationId,
        String eventType,
        String previousStatus,
        String newStatus,
        String actorId,
        String actorRole,
        Instant occurredAt
) {
    public static AuditTimelineEntry from(ReservationEvent event) {
        return new AuditTimelineEntry(
                event.reservationId(), event.type(), event.previousStatus(), event.newStatus(),
                event.actorId(), event.actorRole(), event.timestamp());
    }
}

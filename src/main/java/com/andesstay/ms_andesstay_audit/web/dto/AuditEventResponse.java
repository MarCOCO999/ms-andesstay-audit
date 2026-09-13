package com.andesstay.ms_andesstay_audit.web.dto;

import com.andesstay.ms_andesstay_audit.domain.AuditEvent;

import java.time.Instant;

public record AuditEventResponse(
        Long id,
        String eventId,
        String eventType,
        String reservationId,
        String previousStatus,
        String newStatus,
        String actorId,
        String actorRole,
        Long unitId,
        Instant occurredAt,
        Instant recordedAt
) {
    public static AuditEventResponse from(AuditEvent e) {
        return new AuditEventResponse(
                e.getId(), e.getEventId(), e.getEventType(), e.getReservationId(), e.getPreviousStatus(),
                e.getNewStatus(), e.getActorId(), e.getActorRole(), e.getUnitId(), e.getOccurredAt(), e.getRecordedAt());
    }
}

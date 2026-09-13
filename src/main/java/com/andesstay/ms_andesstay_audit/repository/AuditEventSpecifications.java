package com.andesstay.ms_andesstay_audit.repository;

import com.andesstay.ms_andesstay_audit.domain.AuditEvent;
import org.springframework.data.jpa.domain.Specification;

import java.time.Instant;

public final class AuditEventSpecifications {

    private AuditEventSpecifications() {
    }

    public static Specification<AuditEvent> hasReservationId(String reservationId) {
        return (root, query, cb) -> cb.equal(root.get("reservationId"), reservationId);
    }

    public static Specification<AuditEvent> hasActorId(String actorId) {
        return (root, query, cb) -> cb.equal(root.get("actorId"), actorId);
    }

    public static Specification<AuditEvent> hasEventType(String eventType) {
        return (root, query, cb) -> cb.equal(root.get("eventType"), eventType);
    }

    public static Specification<AuditEvent> occurredFrom(Instant from) {
        return (root, query, cb) -> cb.greaterThanOrEqualTo(root.get("occurredAt"), from);
    }

    public static Specification<AuditEvent> occurredTo(Instant to) {
        return (root, query, cb) -> cb.lessThanOrEqualTo(root.get("occurredAt"), to);
    }
}

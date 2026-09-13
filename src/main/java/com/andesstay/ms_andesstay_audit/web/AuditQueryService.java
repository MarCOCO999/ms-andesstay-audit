package com.andesstay.ms_andesstay_audit.web;

import com.andesstay.ms_andesstay_audit.domain.AuditEvent;
import com.andesstay.ms_andesstay_audit.repository.AuditEventRepository;
import com.andesstay.ms_andesstay_audit.repository.AuditEventSpecifications;
import org.springframework.data.domain.Sort;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

@Service
public class AuditQueryService {

    private final AuditEventRepository repository;

    public AuditQueryService(AuditEventRepository repository) {
        this.repository = repository;
    }

    @Transactional(readOnly = true)
    public List<AuditEvent> search(String reservationId, String actorId, String eventType, Instant from, Instant to) {
        Specification<AuditEvent> spec = (root, query, cb) -> cb.conjunction();
        if (reservationId != null) {
            spec = spec.and(AuditEventSpecifications.hasReservationId(reservationId));
        }
        if (actorId != null) {
            spec = spec.and(AuditEventSpecifications.hasActorId(actorId));
        }
        if (eventType != null) {
            spec = spec.and(AuditEventSpecifications.hasEventType(eventType));
        }
        if (from != null) {
            spec = spec.and(AuditEventSpecifications.occurredFrom(from));
        }
        if (to != null) {
            spec = spec.and(AuditEventSpecifications.occurredTo(to));
        }
        return repository.findAll(spec, Sort.by(Sort.Direction.ASC, "occurredAt"));
    }
}

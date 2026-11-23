package com.andesstay.ms_andesstay_audit.service;

import com.andesstay.ms_andesstay_audit.domain.AuditEvent;
import com.andesstay.ms_andesstay_audit.repository.AuditEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Logica de dominio de la auditoria, sin dependencias de Kafka: persiste cada cambio de estado
 * una sola vez. Kafka entrega "al menos una vez", asi que el mismo evento puede llegar de nuevo
 * (reintento, rebalanceo del consumer group); en ese caso se devuelve el registro ya guardado.
 */
@Service
public class AuditRecordingService {

    private static final Logger log = LoggerFactory.getLogger(AuditRecordingService.class);

    private final AuditEventRepository repository;

    public AuditRecordingService(AuditEventRepository repository) {
        this.repository = repository;
    }

    @Transactional
    public AuditEvent record(AuditEvent event) {
        return repository.findByEventId(event.getEventId())
                .map(existing -> {
                    log.info("Evento {} ya estaba auditado, se ignora el redelivery", existing.getEventId());
                    return existing;
                })
                .orElseGet(() -> {
                    AuditEvent saved = repository.save(event);
                    log.info("Reserva {}: {} -> {} por {} ({}), evento {}", saved.getReservationId(),
                            saved.getPreviousStatus(), saved.getNewStatus(), saved.getActorId(),
                            saved.getActorRole(), saved.getEventId());
                    return saved;
                });
    }
}

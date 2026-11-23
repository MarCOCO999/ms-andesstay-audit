package com.andesstay.ms_andesstay_audit.service;

import com.andesstay.ms_andesstay_audit.domain.AuditEvent;
import com.andesstay.ms_andesstay_audit.repository.AuditEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AuditRecordingServiceTest {

    private AuditEventRepository repository;
    private AuditRecordingService service;

    @BeforeEach
    void setUp() {
        repository = mock(AuditEventRepository.class);
        service = new AuditRecordingService(repository);
    }

    @Test
    void persisteUnEventoNuevo() {
        AuditEvent event = event("e-1");
        when(repository.findByEventId("e-1")).thenReturn(Optional.empty());
        when(repository.save(event)).thenReturn(event);

        assertThat(service.record(event)).isSameAs(event);
        verify(repository).save(event);
    }

    @Test
    void unRedeliveryNoDuplicaLaAuditoria() {
        AuditEvent existing = event("e-1");
        when(repository.findByEventId("e-1")).thenReturn(Optional.of(existing));

        assertThat(service.record(event("e-1"))).isSameAs(existing);
        verify(repository, never()).save(any());
    }

    private static AuditEvent event(String eventId) {
        return AuditEvent.builder().eventId(eventId).eventType("reservation.confirmada").reservationId("10")
                .previousStatus("CREADA").newStatus("CONFIRMADA").actorId("u-1").actorRole("Operador")
                .unitId(3L).occurredAt(Instant.parse("2026-11-08T12:00:00Z")).build();
    }
}

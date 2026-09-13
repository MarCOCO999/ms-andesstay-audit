package com.andesstay.ms_andesstay_audit.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;

/**
 * Registro append-only del timeline de una reserva. Cada fila proviene de un mensaje del
 * topico reservations.events; "eventId" es unico para poder ignorar redelivery de Kafka
 * (at-least-once) sin duplicar la auditoria.
 */
@Entity
@Table(name = "AUDIT_EVENTS")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class AuditEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "EVENT_ID", nullable = false, unique = true, length = 100)
    private String eventId;

    @Column(name = "EVENT_TYPE", nullable = false, length = 60)
    private String eventType;

    @Column(name = "RESERVATION_ID", nullable = false, length = 60)
    private String reservationId;

    @Column(name = "PREVIOUS_STATUS", length = 30)
    private String previousStatus;

    @Column(name = "NEW_STATUS", nullable = false, length = 30)
    private String newStatus;

    /** "quien": el oid/sub del JWT de Azure AD de quien disparo el cambio. */
    @Column(name = "ACTOR_ID", nullable = false, length = 100)
    private String actorId;

    @Column(name = "ACTOR_ROLE", length = 30)
    private String actorRole;

    @Column(name = "UNIT_ID")
    private Long unitId;

    /** "cuando": el timestamp de negocio que trae el evento (no cuando llego a Kafka). */
    @Column(name = "OCCURRED_AT", nullable = false)
    private Instant occurredAt;

    /** Cuando este servicio efectivamente persistio el evento. */
    @Column(name = "RECORDED_AT", nullable = false, updatable = false)
    private Instant recordedAt;

    @PrePersist
    void onCreate() {
        this.recordedAt = Instant.now();
    }
}

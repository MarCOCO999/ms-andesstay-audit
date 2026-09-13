package com.andesstay.ms_andesstay_audit.messaging;

import java.time.Instant;

/**
 * Contrato consumido desde el topico reservations.events, publicado por
 * ms-andesstay-reservations en cada cambio de estado de una reserva (creacion,
 * confirmacion, check-in, check-out, cancelacion). El envelope
 * type/eventId/timestamp/traceId/correlationId es el mismo acordado para las colas de
 * RabbitMQ de notify.
 */
public record ReservationEvent(
        String type,
        String eventId,
        Instant timestamp,
        String traceId,
        String correlationId,
        String reservationId,
        String previousStatus,
        String newStatus,
        String actorId,
        String actorRole,
        Long unitId
) {
}

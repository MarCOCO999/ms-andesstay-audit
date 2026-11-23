package com.andesstay.ms_andesstay_audit.messaging;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;

import java.time.Instant;

/**
 * Contrato consumido desde el topico reservations.events, publicado por
 * ms-andesstay-reservations en cada cambio de estado de una reserva (creacion,
 * confirmacion, check-in, check-out, cancelacion). El envelope
 * type/eventId/timestamp/traceId/correlationId es el mismo acordado para las colas de
 * RabbitMQ de notify.
 * <p>
 * Las restricciones se validan al recibir el mensaje (@Valid en el listener): un evento que no
 * las cumple no se puede auditar y va directo al DLT, sin reintentos.
 */
public record ReservationEvent(
        @NotBlank @Pattern(regexp = "reservation\\.[a-z_]+", message = "debe tener la forma reservation.<accion>")
        String type,
        @NotBlank String eventId,
        @NotNull Instant timestamp,
        String traceId,
        String correlationId,
        @NotBlank String reservationId,
        @Pattern(regexp = ReservationEvent.STATUS_REGEX, message = "no es un estado de reserva valido")
        String previousStatus,
        @NotBlank @Pattern(regexp = ReservationEvent.STATUS_REGEX, message = "no es un estado de reserva valido")
        String newStatus,
        @NotBlank String actorId,
        String actorRole,
        @NotNull @Positive Long unitId
) {
    /** reservations publica Enum.name() (EN_ESTADIA); se acepta tambien la forma con tilde de la API REST. */
    static final String STATUS_REGEX = "CREADA|CONFIRMADA|CHECKIN_PENDIENTE|EN_ESTAD[IÍ]A|CHECKOUT|CANCELADA";
}

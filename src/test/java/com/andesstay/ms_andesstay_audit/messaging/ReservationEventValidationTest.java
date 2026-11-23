package com.andesstay.ms_andesstay_audit.messaging;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.KafkaHeaders;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Reglas que deciden si un evento se audita o se envia directo al DLT. */
class ReservationEventValidationTest {

    private final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();

    @Test
    void unEventoCompletoEsValido() {
        assertThat(validator.validate(event("reservation.created", "e-1", null, "CREADA", 3L))).isEmpty();
    }

    @Test
    void aceptaEnEstadiaConYSinTilde() {
        assertThat(validator.validate(event("reservation.en_estadia", "e-1", "CHECKIN_PENDIENTE", "EN_ESTADIA", 3L))).isEmpty();
        assertThat(validator.validate(event("reservation.en_estadia", "e-1", "CHECKIN_PENDIENTE", "EN_ESTADÍA", 3L))).isEmpty();
    }

    @Test
    void rechazaEstadoDesconocidoTipoMalFormadoYCamposFaltantes() {
        Set<ConstraintViolation<ReservationEvent>> violations =
                validator.validate(event("pago.ok", "", null, "PAGADA", null));

        assertThat(violations).extracting(v -> v.getPropertyPath().toString())
                .containsExactlyInAnyOrder("type", "eventId", "newStatus", "unitId");
    }

    @Test
    void elMonitorDelDltLeeLosHeadersDeOrigen() {
        ConsumerRecord<String, String> record = new ConsumerRecord<>("reservations.events.DLT", 0, 4, "10", "{}");
        record.headers().add(KafkaHeaders.DLT_ORIGINAL_TOPIC, "reservations.events".getBytes(StandardCharsets.UTF_8));
        record.headers().add(KafkaHeaders.DLT_ORIGINAL_PARTITION, ByteBuffer.allocate(4).putInt(2).array());
        record.headers().add(KafkaHeaders.DLT_ORIGINAL_OFFSET, ByteBuffer.allocate(8).putLong(57L).array());

        assertThat(DeadLetterMonitor.header(record, KafkaHeaders.DLT_ORIGINAL_TOPIC)).isEqualTo("reservations.events");
        assertThat(DeadLetterMonitor.header(record, KafkaHeaders.DLT_ORIGINAL_PARTITION)).isEqualTo("2");
        assertThat(DeadLetterMonitor.header(record, KafkaHeaders.DLT_ORIGINAL_OFFSET)).isEqualTo("57");
        assertThat(DeadLetterMonitor.header(record, KafkaHeaders.DLT_EXCEPTION_MESSAGE)).isEqualTo("?");
    }

    private static ReservationEvent event(String type, String eventId, String previous, String next, Long unitId) {
        return new ReservationEvent(type, eventId, Instant.now(), "t", "c", "10", previous, next, "u-1", "Cliente", unitId);
    }
}

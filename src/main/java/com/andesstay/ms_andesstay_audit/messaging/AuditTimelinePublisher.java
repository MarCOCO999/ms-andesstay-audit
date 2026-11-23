package com.andesstay.ms_andesstay_audit.messaging;

import com.andesstay.ms_andesstay_audit.config.KafkaTopicsProperties;
import com.andesstay.ms_andesstay_audit.domain.AuditEvent;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Productor de audit.timeline. El envio es sincronico (espera el ack del broker) porque se hace
 * desde el listener de reservations.events: si falla, la excepcion hace que el error handler
 * reintente el evento completo, y como la auditoria es idempotente por eventId, en el reintento
 * solo se vuelve a publicar la entrada del timeline.
 */
@Component
public class AuditTimelinePublisher {

    private static final Logger log = LoggerFactory.getLogger(AuditTimelinePublisher.class);
    private static final Duration SEND_TIMEOUT = Duration.ofSeconds(10);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String topic;

    public AuditTimelinePublisher(KafkaTemplate<String, Object> kafkaTemplate, KafkaTopicsProperties topics) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topics.auditTimeline().name();
    }

    public void publish(AuditEvent auditEvent) {
        AuditTimelineEntry entry = AuditTimelineEntry.from(auditEvent);
        try {
            RecordMetadata metadata = kafkaTemplate.send(topic, entry.reservationId(), entry)
                    .get(SEND_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                    .getRecordMetadata();
            log.debug("Timeline de la reserva {} publicado en {}-{}@{}",
                    entry.reservationId(), metadata.topic(), metadata.partition(), metadata.offset());
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new TimelinePublishException(entry, ex);
        } catch (ExecutionException | TimeoutException ex) {
            throw new TimelinePublishException(entry, ex);
        }
    }

    /** El broker no confirmo la entrada del timeline: es transitorio, el error handler la reintenta. */
    public static class TimelinePublishException extends RuntimeException {
        TimelinePublishException(AuditTimelineEntry entry, Throwable cause) {
            super("No se pudo publicar en audit.timeline el evento " + entry.eventId()
                    + " de la reserva " + entry.reservationId(), cause);
        }
    }
}

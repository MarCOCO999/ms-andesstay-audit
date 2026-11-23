package com.andesstay.ms_andesstay_audit.messaging;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Deja en el log (nivel ERROR) cada mensaje que llega al DLT de reservations.events, con el
 * topico, particion, offset y consumer group de origen y el motivo del fallo que agrega
 * DeadLetterPublishingRecoverer. Tiene su propio consumer group, asi que no compite con el
 * procesamiento normal, y lee el valor como texto para mostrar incluso un JSON que no se pudo
 * deserializar.
 */
@Component
public class DeadLetterMonitor {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterMonitor.class);

    @KafkaListener(
            id = "audit-dlt-monitor",
            topics = "${andesstay.kafka.topics.reservations-events-dlt.name}",
            groupId = "${andesstay.kafka.consumer-groups.dlt-monitor}",
            concurrency = "1",
            properties = "value.deserializer=org.apache.kafka.common.serialization.StringDeserializer")
    public void onDeadLetter(ConsumerRecord<String, String> record) {
        log.error("Mensaje en DLT {}-{}@{} | origen {}-{}@{} (grupo {}) | key={} | causa: {} - {} | valor: {}",
                record.topic(), record.partition(), record.offset(),
                header(record, KafkaHeaders.DLT_ORIGINAL_TOPIC), header(record, KafkaHeaders.DLT_ORIGINAL_PARTITION),
                header(record, KafkaHeaders.DLT_ORIGINAL_OFFSET), header(record, KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP),
                record.key(), simpleName(header(record, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN)),
                header(record, KafkaHeaders.DLT_EXCEPTION_MESSAGE), record.value());
    }

    private static String simpleName(String className) {
        return className.substring(className.lastIndexOf('.') + 1);
    }

    static String header(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        if (header == null || header.value() == null) {
            return "?";
        }
        byte[] value = header.value();
        // La particion y el offset de origen vienen como int/long binarios, no como texto.
        if (KafkaHeaders.DLT_ORIGINAL_PARTITION.equals(name) && value.length == Integer.BYTES) {
            return String.valueOf(ByteBuffer.wrap(value).getInt());
        }
        if (KafkaHeaders.DLT_ORIGINAL_OFFSET.equals(name) && value.length == Long.BYTES) {
            return String.valueOf(ByteBuffer.wrap(value).getLong());
        }
        return new String(value, StandardCharsets.UTF_8);
    }
}

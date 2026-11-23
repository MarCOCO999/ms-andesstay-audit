package com.andesstay.ms_andesstay_audit.config;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.messaging.handler.annotation.support.MethodArgumentNotValidException;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Manejo de errores de todos los @KafkaListener del servicio (Spring Boot lo asigna a la
 * container factory por defecto):
 * <ul>
 *   <li><b>Errores transitorios</b> (base de datos o broker caido un momento): se reintenta el
 *       mismo registro con backoff exponencial (andesstay.kafka.consumer.retry), sin perder el
 *       orden de la particion. Cada intento fallido queda en el log con topico, particion,
 *       offset, key y causa.</li>
 *   <li><b>Mensajes defectuosos</b>: JSON que no se puede deserializar (ErrorHandlingDeserializer)
 *       o payload que no pasa la validacion (@Valid sobre el DTO). Reintentarlos no los arregla,
 *       asi que van directo al DLT sin esperar.</li>
 *   <li><b>DLT</b>: agotados los reintentos, el registro se publica en "&lt;topico&gt;.DLT" con los
 *       headers de diagnostico que agrega Spring (kafka_dlt-exception-message, -original-topic,
 *       -original-offset...) y el consumidor sigue con el siguiente: un mensaje malo nunca
 *       detiene la particion.</li>
 * </ul>
 * El destino se fija explicitamente porque el sufijo por defecto de DeadLetterPublishingRecoverer
 * es "-dlt", no ".DLT" como pide el caso; la particion -1 deja que el productor la elija (los DLT
 * tienen 1 sola particion, no las mismas que el topico de origen).
 */
@Configuration
public class KafkaConsumerConfig implements DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfig.class);

    private DefaultKafkaProducerFactory<String, Object> deadLetterProducerFactory;

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(ProducerFactory<?, ?> producerFactory, KafkaConsumerProperties consumer) {
        DeadLetterPublishingRecoverer deadLetterPublisher = new DeadLetterPublishingRecoverer(
                deadLetterTemplate(producerFactory),
                (record, ex) -> new TopicPartition(record.topic() + ".DLT", -1));

        KafkaConsumerProperties.Retry retry = consumer.retry();
        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(retry.maxRetries());
        backOff.setInitialInterval(retry.initialIntervalMs());
        backOff.setMultiplier(retry.multiplier());
        backOff.setMaxInterval(retry.maxIntervalMs());

        DefaultErrorHandler handler = new DefaultErrorHandler((record, ex) -> {
            log.error("Enviando a {}.DLT {}: {}", record.topic(), describe(record), rootCause(ex));
            deadLetterPublisher.accept(record, ex);
        }, backOff);
        handler.setRetryListeners(new RetryListener() {
            @Override
            public void failedDelivery(ConsumerRecord<?, ?> record, Exception ex, int deliveryAttempt) {
                log.warn("Intento {} de {} fallido para {}: {}",
                        deliveryAttempt, retry.maxRetries() + 1, describe(record), rootCause(ex));
            }

            @Override
            public void recoveryFailed(ConsumerRecord<?, ?> record, Exception original, Exception failure) {
                log.error("No se pudo enviar al DLT {}: {} (se reintentara el registro)", describe(record), rootCause(failure));
            }
        });
        return handler;
    }

    /**
     * Template propio para el DLT: si el mensaje original no se pudo deserializar, el recoverer
     * publica sus bytes crudos (byte[]) y deben salir tal cual, no como un string JSON en base64;
     * los DTO que si se deserializaron (pero no pasaron la validacion) salen como JSON.
     */
    private KafkaTemplate<String, Object> deadLetterTemplate(ProducerFactory<?, ?> producerFactory) {
        Map<String, Object> config = new HashMap<>(producerFactory.getConfigurationProperties());
        config.computeIfPresent("client.id", (key, clientId) -> clientId + "-dlt");
        // El serializador JSON se configura via properties (no con setters: no admite ambas cosas).
        config.put(JacksonJsonSerializer.ADD_TYPE_INFO_HEADERS, false);

        Map<Class<?>, Serializer<?>> delegates = new LinkedHashMap<>();
        delegates.put(byte[].class, new ByteArraySerializer());
        delegates.put(Object.class, new JacksonJsonSerializer<>());

        deadLetterProducerFactory = new DefaultKafkaProducerFactory<>(config,
                new StringSerializer(), new DelegatingByTypeSerializer(delegates, true));
        return new KafkaTemplate<>(deadLetterProducerFactory);
    }

    private static String describe(ConsumerRecord<?, ?> record) {
        return record.topic() + "-" + record.partition() + "@" + record.offset() + " (key=" + record.key() + ")";
    }

    private static String rootCause(Throwable ex) {
        Throwable root = NestedExceptionUtils.getMostSpecificCause(ex);
        // Payload invalido: solo "campo: motivo (valor rechazado)", sin el volcado completo del binding.
        if (root instanceof MethodArgumentNotValidException invalid && invalid.getBindingResult() != null) {
            return "payload invalido: " + invalid.getBindingResult().getFieldErrors().stream()
                    .map(error -> error.getField() + ": " + error.getDefaultMessage() + " (" + error.getRejectedValue() + ")")
                    .collect(Collectors.joining("; "));
        }
        return root.getClass().getSimpleName() + ": " + root.getMessage();
    }

    @Override
    public void destroy() {
        if (deadLetterProducerFactory != null) {
            deadLetterProducerFactory.destroy();
        }
    }
}

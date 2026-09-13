package com.andesstay.ms_andesstay_audit.config;

import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * "Mensajes que fallaron tras N reintentos, con metadatos de error" (caso, seccion 9):
 * 2 reintentos con 2s de espera y, si el tercer intento tambien falla, el registro se
 * publica en "<topico-origen>.DLT". Se fija el resolver de destino explicitamente porque
 * el sufijo por defecto de DeadLetterPublishingRecoverer en esta version es "-dlt" (minuscula,
 * con guion), no ".DLT" como pide el caso; la particion se deja en -1 para que el producer
 * la elija (evita el intento de reusar la particion de origen, que puede no existir en el DLT).
 */
@Configuration
public class KafkaConsumerConfig {

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, Object> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (record, ex) -> new TopicPartition(record.topic() + ".DLT", -1));
        return new DefaultErrorHandler(recoverer, new FixedBackOff(2000L, 2L));
    }
}

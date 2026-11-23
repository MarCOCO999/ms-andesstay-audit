package com.andesstay.ms_andesstay_audit.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListenerConfigurer;
import org.springframework.kafka.config.KafkaListenerEndpointRegistrar;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;

/**
 * Habilita @Valid en los parametros @Payload de los @KafkaListener con el validador de Bean
 * Validation de Spring Boot. Un payload invalido lanza MethodArgumentNotValidException, que el
 * DefaultErrorHandler clasifica como no reintentable: va directo al DLT.
 */
@Configuration
public class KafkaListenerValidationConfig implements KafkaListenerConfigurer {

    private final LocalValidatorFactoryBean validator;

    public KafkaListenerValidationConfig(LocalValidatorFactoryBean validator) {
        this.validator = validator;
    }

    @Override
    public void configureKafkaListeners(KafkaListenerEndpointRegistrar registrar) {
        registrar.setValidator(validator);
    }
}

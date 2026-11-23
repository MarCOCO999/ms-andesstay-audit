package com.andesstay.ms_andesstay_audit.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.kafka.config.TopicBuilder;

import java.util.HashMap;
import java.util.Map;

/**
 * Definicion de un topico (andesstay.kafka.topics.* en application.yml): nombre, particiones,
 * factor de replicacion y configuracion propia (retencion, cleanup.policy, min.insync.replicas...).
 * Los mismos valores estan en infra/kafka/scripts/create-topics.sh.
 */
public record TopicSpec(String name, int partitions, int replicas, Map<String, String> configs) {

    public TopicSpec {
        configs = configs == null ? Map.of() : Map.copyOf(configs);
    }

    /**
     * min.insync.replicas mayor que el factor de replicacion haria fallar toda escritura con
     * acks=all (p.ej. si el topico se crea con 1 replica en un cluster de un solo broker), asi
     * que se acota al numero de replicas.
     */
    public NewTopic toNewTopic() {
        Map<String, String> effective = new HashMap<>(configs);
        String minIsr = effective.get(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG);
        if (minIsr != null && Integer.parseInt(minIsr) > replicas) {
            effective.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, String.valueOf(replicas));
        }
        return TopicBuilder.name(name).partitions(partitions).replicas(replicas).configs(effective).build();
    }
}

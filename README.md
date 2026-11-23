# ms-andesstay-audit

Microservicio de auditoría del proyecto **AndesStay** (caso DSY1107 – Desarrollo Cloud Native I, Duoc UC): consume el stream de eventos de reservas de **Kafka**, los persiste como una línea de tiempo normalizada y expone una API de solo lectura para revisarlos.

## Rol en la arquitectura

- Consume el tópico `reservations.events` (publicado por `ms-andesstay-reservations` cada vez que se crea una reserva o cambia de estado).
- Persiste cada evento en Oracle y lo republica en el tópico `audit.timeline`, ya normalizado (key = `reservationId`), que consume `ms-andesstay-report`.
- Expone una API **solo de lectura** (`/api/audit`) para los roles `Admin` y `Auditor`; no hay endpoints de escritura, la única forma de que aparezca un registro es a través del listener de Kafka.

## Kafka

| Pieza | Clase | Detalle |
|---|---|---|
| Consumidor | `messaging/ReservationEventListener` | `@KafkaListener` de `reservations.events`, grupo `ms-andesstay-audit`, 3 hilos (1 por partición). Solo adapta: valida el DTO, lo traduce a la entidad y delega en `service/AuditRecordingService` (lógica de dominio, sin Kafka). |
| Productor | `messaging/AuditTimelinePublisher` | Publica `AuditTimelineEntry` en `audit.timeline` con envío sincrónico: si el broker no confirma, el evento completo se reintenta. |
| Tópicos | `config/KafkaTopicsConfig` | Declara `audit.timeline` (compactado) y `reservations.events.DLT` con la configuración de `andesstay.kafka.topics` en `application.yml`. |
| Errores | `config/KafkaConsumerConfig` | Ver abajo. |
| Monitor DLT | `messaging/DeadLetterMonitor` | Grupo propio (`ms-andesstay-audit-dlt-monitor`): registra en el log cada mensaje que llega al DLT con su origen, grupo y causa. |

**Manejo de errores** (`DefaultErrorHandler`):

- **Payload inválido** (`@Valid` sobre `ReservationEvent`: estado desconocido, campos faltantes) o **JSON ilegible** (`ErrorHandlingDeserializer`): no se reintentan, van directo a `reservations.events.DLT`.
- **Errores transitorios** (base de datos o broker caído un momento): 3 reintentos con backoff exponencial (1 s, 2 s, 4 s; `andesstay.kafka.consumer.retry`) y luego al DLT.
- Cada intento fallido y cada envío al DLT quedan en el log con tópico, partición, offset, key y causa; el DLT conserva los headers de diagnóstico (`kafka_dlt-original-topic`, `-offset`, `-exception-message`…). Un mensaje malo nunca detiene la partición.
- **Idempotencia**: Kafka entrega "al menos una vez"; un `eventId` ya auditado no se duplica (solo se vuelve a publicar su entrada del timeline).

**Cliente**: `acks=all` + idempotencia en el productor; en el consumidor `enable-auto-commit=false` con `ack-mode: record` (el offset se confirma después de procesar), `CooperativeStickyAssignor` (rebalanceo incremental) e `isolation.level=read_committed`.

## Stack

- Spring Boot 4.1.1 / Java 25 (LTS)
- Spring Data JPA + Oracle Database (driver `ojdbc17`)
- Spring Kafka (consumidor de `reservations.events`, productor de `audit.timeline` y del `.DLT`)
- Spring Security OAuth2 Resource Server (JWT de Azure AD)

## Configuración

| Variable | Descripción | Default |
|---|---|---|
| `SERVER_PORT` | Puerto HTTP | `8084` |
| `DB_URL` / `DB_USERNAME` / `DB_PASSWORD` | Conexión a Oracle | `jdbc:oracle:thin:@localhost:1521/FREEPDB1` / `andesstay` / `changeit` |
| `KAFKA_BOOTSTRAP_SERVERS` | Brokers de Kafka | `localhost:9092,localhost:9093,localhost:9094` |
| `KAFKA_TOPIC_REPLICATION_FACTOR` / `KAFKA_MIN_INSYNC_REPLICAS` | Réplicas y `min.insync.replicas` de los tópicos que declara | `3` / `2` (usar `1` / `1` con un Kafka de un solo broker) |
| `AZURE_ISSUER_URI` / `AZURE_API_AUDIENCE` | Validación del JWT de Azure AD | tenant/App ID URI del caso |

## Endpoints principales

| Método | Ruta | Roles | Descripción |
|---|---|---|---|
| `GET` | `/api/audit/events` | Admin, Auditor | Búsqueda de eventos (filtros por reserva, actor, tipo y rango de fechas) |
| `GET` | `/api/audit/events/reservation/{reservationId}` | Admin, Auditor | Línea de tiempo completa de una reserva |

## Cómo ejecutarlo en local

Requiere Java 25, Oracle y Kafka+Zookeeper levantados (ver [andesstay-infra](https://github.com/MarCOCO999/andesstay-infra)).

```bash
./mvnw spring-boot:run
```

## Repos relacionados

[frontend-andesstay](https://github.com/MarCOCO999/frontend-andesstay) · [ms-andesstay-bff](https://github.com/MarCOCO999/ms-andesstay-bff) · [ms-andesstay-reservations](https://github.com/MarCOCO999/ms-andesstay-reservations) · [ms-andesstay-catalog](https://github.com/MarCOCO999/ms-andesstay-catalog) · [ms-andesstay-notify](https://github.com/MarCOCO999/ms-andesstay-notify) · [ms-andesstay-report](https://github.com/MarCOCO999/ms-andesstay-report) · [ms-andesstay-rabbit-admin](https://github.com/MarCOCO999/ms-andesstay-rabbit-admin) · [ms-andesstay-kafka-admin](https://github.com/MarCOCO999/ms-andesstay-kafka-admin) · [andesstay-infra](https://github.com/MarCOCO999/andesstay-infra)

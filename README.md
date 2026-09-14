# ms-andesstay-audit

Microservicio de auditoría del proyecto **AndesStay** (caso DSY1107 – Desarrollo Cloud Native I, Duoc UC): consume el stream de eventos de reservas de **Kafka**, los persiste como una línea de tiempo normalizada y expone una API de solo lectura para revisarlos.

## Rol en la arquitectura

- Consume el tópico `reservations.events` (publicado por `ms-andesstay-reservations` cada vez que se crea una reserva o cambia de estado).
- Persiste cada evento en Oracle y lo republica en el tópico `audit.timeline`, ya normalizado.
- Expone una API **solo de lectura** (`/api/audit`) para los roles `Admin` y `Auditor`; no hay endpoints de escritura, la única forma de que aparezca un registro es a través del listener de Kafka.
- Manejo de errores con reintentos: si el procesamiento de un mensaje falla, se reintenta 2 veces con 2s de espera; si el tercer intento también falla, el mensaje se publica en `reservations.events.DLT` con sus metadatos de error, para no perder el registro ni bloquear el consumo del resto del tópico.

## Stack

- Spring Boot 4.1.1 / Java 21
- Spring Data JPA + Oracle Database (driver `ojdbc17`)
- Spring Kafka (consumidor de `reservations.events`, productor de `audit.timeline` y del `.DLT`)
- Spring Security OAuth2 Resource Server (JWT de Azure AD)

## Configuración

| Variable | Descripción | Default |
|---|---|---|
| `SERVER_PORT` | Puerto HTTP | `8084` |
| `DB_URL` / `DB_USERNAME` / `DB_PASSWORD` | Conexión a Oracle | `jdbc:oracle:thin:@localhost:1521/FREEPDB1` / `andesstay` / `changeit` |
| `KAFKA_BOOTSTRAP_SERVERS` | Broker de Kafka | `localhost:9092` |
| `KAFKA_TOPIC_REPLICATION_FACTOR` | Factor de replicación de los tópicos propios | `1` (el caso pide 3 en el clúster de EC2 de evaluación) |
| `AZURE_ISSUER_URI` / `AZURE_API_AUDIENCE` | Validación del JWT de Azure AD | tenant/App ID URI del caso |

## Endpoints principales

| Método | Ruta | Roles | Descripción |
|---|---|---|---|
| `GET` | `/api/audit/events` | Admin, Auditor | Búsqueda de eventos (filtros por reserva, actor, tipo y rango de fechas) |
| `GET` | `/api/audit/events/reservation/{reservationId}` | Admin, Auditor | Línea de tiempo completa de una reserva |

## Cómo ejecutarlo en local

Requiere Java 21, Oracle y Kafka+Zookeeper levantados (ver [andesstay-infra](https://github.com/MarCOCO999/andesstay-infra)).

```bash
./mvnw spring-boot:run
```

## Repos relacionados

[frontend-andesstay](https://github.com/MarCOCO999/frontend-andesstay) · [ms-andesstay-bff](https://github.com/MarCOCO999/ms-andesstay-bff) · [ms-andesstay-reservations](https://github.com/MarCOCO999/ms-andesstay-reservations) · [ms-andesstay-catalog](https://github.com/MarCOCO999/ms-andesstay-catalog) · [ms-andesstay-notify](https://github.com/MarCOCO999/ms-andesstay-notify) · [ms-andesstay-report](https://github.com/MarCOCO999/ms-andesstay-report) · [andesstay-infra](https://github.com/MarCOCO999/andesstay-infra)

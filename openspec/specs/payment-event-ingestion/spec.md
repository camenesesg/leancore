# payment-event-ingestion Specification

## Purpose

Recibe eventos de pago (procesado o fallido), los encola en una cola en memoria acotada y los consume continuamente para alimentar la agregación de estadísticas.

## Requirements

### Requirement: Payment event format
Un evento de pago SHALL ser un objeto JSON con los campos:
- `type` (obligatorio): `payment.processed` o `payment.failed`.
- `id` (obligatorio): identificador no vacío del pago.
- `amount` (opcional): número mayor o igual a 0.
- `occurred_at` (opcional): timestamp RFC 3339. Si falta, el sistema MUST usar la hora de recepción.

El sistema MUST rechazar un evento cuyo `occurred_at` esté más de 1 minuto en el futuro respecto a la hora del servidor.

#### Scenario: Event without occurred_at
- **WHEN** se recibe un evento válido sin `occurred_at`
- **THEN** el evento se registra con la hora de recepción como `occurred_at`

#### Scenario: Event too far in the future
- **WHEN** se recibe un evento con `occurred_at` 5 minutos en el futuro
- **THEN** el evento es rechazado como inválido

### Requirement: Publish events over HTTP
El sistema SHALL exponer `POST /api/events` que acepta un evento de pago en el cuerpo JSON y lo publica en la cola.
- Evento válido encolado: MUST responder `202 Accepted`.
- JSON malformado, más de un valor JSON en el cuerpo, `type` desconocido, `id` vacío, `amount` negativo o `occurred_at` inválido: MUST responder `400 Bad Request` con un cuerpo JSON `{"error": "<mensaje>"}` y no encolar nada.
- Campos JSON desconocidos: MUST ignorarse (el evento se acepta si el resto es válido).
- Cuerpo de más de 1 MiB: MUST responder `413 Payload Too Large` con cuerpo JSON de error y no encolar nada.
- Cola llena o servicio apagándose (cola cerrada): MUST responder `503 Service Unavailable` con cuerpo JSON de error, sin bloquear la petición.

#### Scenario: Valid event accepted
- **WHEN** un cliente envía `{"type":"payment.processed","id":"p-1"}`
- **THEN** la respuesta es `202` y el evento queda en la cola

#### Scenario: Unknown type rejected
- **WHEN** un cliente envía `{"type":"payment.refunded","id":"p-2"}`
- **THEN** la respuesta es `400` con un mensaje de error y la cola no cambia

#### Scenario: Queue full
- **WHEN** la cola está en su capacidad máxima y llega un evento válido
- **THEN** la respuesta es `503` y el evento no se encola

#### Scenario: Body too large
- **WHEN** un cliente envía un cuerpo de más de 1 MiB
- **THEN** la respuesta es `413` con un mensaje de error y la cola no cambia

#### Scenario: Service shutting down
- **WHEN** el servicio ya inició el apagado (la cola está cerrada) y llega un evento válido
- **THEN** la respuesta es `503` y el evento no se encola

### Requirement: Bounded in-memory queue
La cola SHALL residir en memoria del proceso y tener una capacidad máxima configurable (por defecto 1000 eventos). Los eventos se consumen en orden FIFO.

#### Scenario: Capacity is configurable
- **WHEN** el servicio arranca con capacidad 2 y se publican 3 eventos sin consumir
- **THEN** los dos primeros se aceptan y el tercero se rechaza por cola llena

### Requirement: Continuous consumption
El sistema SHALL consumir eventos de la cola de forma continua mientras el servicio esté en ejecución y entregarlos a la agregación de estadísticas, de modo que un evento aceptado se refleje en `GET /api/stats` en menos de 1 segundo en condiciones normales.

#### Scenario: Accepted event becomes visible
- **WHEN** se acepta un evento `payment.failed` con `occurred_at` en el minuto actual
- **THEN** en menos de 1 segundo el conteo `failed` del minuto actual en `GET /api/stats` aumenta en 1

### Requirement: Graceful shutdown
Al recibir SIGINT o SIGTERM, el sistema SHALL dejar de aceptar nuevas peticiones, procesar los eventos ya encolados y terminar.

#### Scenario: Pending events drained on shutdown
- **WHEN** hay eventos en la cola y el proceso recibe SIGTERM
- **THEN** esos eventos se entregan a la agregación antes de que el proceso termine

## Test Cases

### TC-ING-01: Accept a valid processed event (positive)
- **Given** el servicio en ejecución con la cola vacía
- **When** se hace `POST /api/events` con `{"type":"payment.processed","id":"p-1","amount":10.5}`
- **Then** la respuesta es `202` y, tras el consumo, el minuto actual tiene `processed = 1`

### TC-ING-02: Reject malformed JSON (negative)
- **Given** el servicio en ejecución
- **When** se hace `POST /api/events` con el cuerpo `{"type":`
- **Then** la respuesta es `400` con `{"error": ...}` y ningún conteo cambia

### TC-ING-03: Reject unknown event type (negative)
- **Given** el servicio en ejecución
- **When** se hace `POST /api/events` con `{"type":"payment.refunded","id":"p-2"}`
- **Then** la respuesta es `400` y la cola sigue vacía

### TC-ING-04: Backpressure when queue is full (negative)
- **Given** una cola con capacidad 1 que ya contiene un evento y sin consumidor activo
- **When** se hace `POST /api/events` con un evento válido
- **Then** la respuesta es `503` y la cola sigue con 1 evento

### TC-ING-05: Drain queue on shutdown (positive)
- **Given** 3 eventos válidos encolados y aún no consumidos
- **When** el servicio recibe la señal de apagado
- **Then** los 3 eventos quedan reflejados en la agregación antes de terminar

### TC-ING-06: Reject oversized body (negative)
- **Given** el servicio en ejecución con la cola vacía
- **When** se hace `POST /api/events` con un cuerpo JSON de más de 1 MiB
- **Then** la respuesta es `413` con `{"error": ...}` y la cola sigue vacía

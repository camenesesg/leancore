# Proposal

## Why

No hay forma de ver de un vistazo cómo se están resolviendo los pagos. Necesitamos un servicio que consuma eventos de pago desde una cola y muestre, casi en tiempo real, cuántos pagos se procesaron o fallaron en cada minuto, para detectar picos de fallos sin revisar logs.

## What Changes

- Nuevo servicio Go (`cmd/dashboard`) sin dependencias externas en tiempo de ejecución.
- Cola de eventos **en memoria** (acotada, con backpressure) detrás de una interfaz `Queue`.
- Worker consumidor que drena la cola y alimenta un agregador.
- Agregador por minuto (UTC) con conteos de `payment.processed` y `payment.failed` y ventana de retención configurable.
- `POST /api/events`: publica un evento en la cola (punto de entrada de productores).
- `GET /api/stats`: devuelve JSON con los conteos por minuto de los últimos N minutos y los totales.
- `GET /`: página HTML simple que consulta `/api/stats` cada ~2 s y muestra éxitos vs. fallos por minuto.
- Simulador opcional (`-simulate`) que genera eventos aleatorios para demo.

## Capabilities

### New Capabilities
- `payment-event-ingestion`: formato del evento de pago, publicación vía HTTP en la cola en memoria, validación, backpressure y consumo continuo hacia el agregador.
- `payment-stats-dashboard`: agregación por minuto con retención, endpoint JSON de estadísticas y página que se actualiza por polling.

### Modified Capabilities
- Ninguna (el proyecto no tiene specs existentes).

## Non-goals

- Cola Redis o cualquier cola durable/distribuida (la interfaz `Queue` deja la puerta abierta para un cambio posterior).
- Persistir los conteos entre reinicios del proceso.
- Autenticación/autorización de los endpoints.
- Agregación entre múltiples instancias del servicio.
- Push en tiempo real (SSE/WebSockets); se usa polling.
- Librerías de gráficos, SPA o build de frontend.
- Otros tipos de evento además de pago procesado/fallido.

## Impact

- Código nuevo: módulo Go con `cmd/dashboard` e `internal/{event,queue,aggregator,httpapi,simulator}`.
- APIs nuevas: `POST /api/events`, `GET /api/stats`, `GET /`.
- Dependencias: solo librería estándar de Go (1.22+ por los patrones de ruta con método).
- Sin impacto en sistemas existentes (proyecto greenfield).

# Tasks

## 1. Bootstrap del proyecto

- [x] 1.1 Asegurar un toolchain Go ≥ 1.22 (local o con la imagen `golang:1.22` en Docker) y verificarlo con `go version`
- [x] 1.2 Crear el módulo Go (`go mod init`) y los directorios `cmd/dashboard` e `internal/{event,queue,aggregator,consumer,httpapi,simulator}`; verificar que `go build ./...` compila con un `main` vacío

## 2. Modelo de evento y validación

- [x] 2.1 Implementar `event.Event`, las constantes de tipo (`payment.processed`, `payment.failed`) y `Validate(now)` (id no vacío, amount ≥ 0, tipo conocido, `occurred_at` por defecto = now, rechazo si está > 1 min en el futuro); verificar con tests de tabla que cubran casos válidos e inválidos

## 3. Cola en memoria con backpressure

- [x] 3.1 Definir la interfaz `queue.Queue` y los errores `ErrQueueFull` y `ErrClosed`, e implementar `MemoryQueue` sobre un canal con buffer; verificar con tests de orden FIFO, cola llena (TC-ING-04) y Consume bloqueante con cancelación por ctx
- [x] 3.2 Implementar `Close()` con drenado (Consume devuelve los eventos pendientes y luego `ErrClosed`; Publish después de Close falla); verificar con un test ejecutado con `-race`

## 4. Agregador por minuto

- [x] 4.1 Implementar `aggregator.Aggregator` con reloj inyectable, `Record` (bucket por minuto UTC, ignora eventos fuera de retención, poda de buckets vencidos) y `Snapshot(n)` (rellena con ceros, orden ascendente, totales); verificar con tests TC-DASH-01, TC-DASH-02 y TC-DASH-04
- [x] 4.2 Añadir un test de concurrencia (varios `Record` y `Snapshot` en paralelo) y verificar que `go test -race ./internal/aggregator` pasa

## 5. Consumidor y apagado ordenado

- [x] 5.1 Implementar `consumer.Run(ctx, q, agg)`, que consume hasta `ErrClosed` o la cancelación del ctx; verificar con un test en el que eventos publicados aparecen en el agregador y, tras `Close()`, se drenan todos (TC-ING-05)

## 6. API HTTP

- [x] 6.1 Implementar `POST /api/events` (MaxBytesReader, decodificación, validación, 202/400/503 con errores JSON); verificar con tests `httptest` TC-ING-01, TC-ING-02, TC-ING-03 y TC-ING-04
- [x] 6.2 Implementar `GET /api/stats` (parámetro `minutes` 1–60, por defecto 15, 400 si es inválido, formato JSON según la spec); verificar con tests `httptest` que incluyan TC-DASH-03 y la forma exacta del JSON

## 7. Página del dashboard

- [x] 7.1 Crear `internal/httpapi/web/index.html` (HTML/CSS/JS vanilla, polling cada 2 s a `/api/stats`, tabla con barras de éxitos/fallos por minuto, totales, hora de la última actualización y badge de error) embebido con `go:embed` y servido en `GET /`; verificar con el test TC-DASH-05 y abriendo la página en el navegador

## 8. Simulador de eventos

- [x] 8.1 Implementar `simulator.Run(ctx, q, rate, failureRatio)`, que publica eventos aleatorios y registra en log los rechazos por cola llena; verificar con un test que, con un rate alto y un ctx corto, publica eventos de ambos tipos

## 9. Wiring, documentación e integración

- [x] 9.1 Conectar todo en `cmd/dashboard/main.go` (flags `-addr`, `-queue-size`, `-retention`, `-simulate`, `-simulate-rate`, `-simulate-failure-ratio`; `slog`; `signal.NotifyContext`; secuencia de apagado del design); verificar que `go run ./cmd/dashboard -simulate` arranca y que Ctrl+C termina limpio
- [x] 9.2 Escribir el `README.md` con cómo ejecutar, ejemplos de `curl` para publicar eventos y consultar stats, y la nota de que no hay autenticación; verificar ejecutando los comandos tal como están escritos
- [x] 9.3 Escribir un test end-to-end (servidor `httptest` + cola + consumidor reales): publicar N eventos procesados y M fallidos y verificar que `/api/stats` refleja N/M en menos de 1 s; verificar que `go test -race ./...` y `go vet ./...` pasan

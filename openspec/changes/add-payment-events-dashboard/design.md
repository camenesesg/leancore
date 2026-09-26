# Design

## Context

Proyecto greenfield: no hay código Go todavía, solo el andamiaje de OpenSpec. El stack del proyecto es Go (guía de estilo de Google) y Redis, pero este cambio usa deliberadamente una cola **en memoria** (ver proposal.md, Non-goals). Los requisitos observables están en `specs/payment-event-ingestion` y `specs/payment-stats-dashboard`.

## Goals / Non-Goals

**Goals:**
- Un único binario, solo con la librería estándar, que se ejecute con `go run ./cmd/dashboard`.
- Límites claros entre cola, consumidor, agregador y HTTP para que cada pieza se pruebe por separado.
- Tiempo inyectable en el agregador para tests deterministas.
- Poder cambiar la cola por Redis Streams más adelante sin tocar el agregador ni el HTTP.

**Non-Goals:**
- Garantías de entrega at-least-once o de idempotencia (un `id` duplicado se cuenta dos veces).
- Métricas/observabilidad más allá de logs estructurados.

## Decisions

### Estructura de paquetes
```
cmd/dashboard/main.go        // flags, wiring, señales, apagado
internal/event/              // type Event, Type, Validate(now)
internal/queue/              // interface Queue; MemoryQueue
internal/aggregator/         // Aggregator: Record, Snapshot
internal/consumer/           // Run(ctx, q, agg): bucle de consumo
internal/httpapi/            // handlers + web/index.html embebido
internal/simulator/          // generador opcional de eventos
```
Alternativa: un solo paquete `main`. Se descarta porque dificulta probar por separado y el reemplazo futuro de la cola.

### Cola: canal con buffer detrás de una interfaz
```go
type Queue interface {
    Publish(ctx context.Context, e event.Event) error // ErrQueueFull si no hay espacio
    Consume(ctx context.Context) (event.Event, error)  // bloquea hasta evento o ctx.Done
    Close()                                            // no admite más Publish; Consume drena y luego devuelve ErrClosed
}
```
`MemoryQueue` usa `chan event.Event` con capacidad `-queue-size`. `Publish` hace `select` con `default` → `ErrQueueFull` (backpressure sin bloquear el handler → 503). Alternativa: slice + `sync.Cond`; más código sin beneficio.

### Agregador
- `map[time.Time]*bucket` con clave `occurredAt.UTC().Truncate(time.Minute)`, protegido por `sync.Mutex` (escrituras frecuentes y lecturas cada 2 s por cliente; un `RWMutex` no aporta).
- `now func() time.Time` inyectado. La retención se trunca a minutos enteros (mínimo 1) y la expone `RetentionMinutes()`. La ventana va desde `minutoActual − (R−1)·1m` hasta `now + 1m`; `Record` ignora lo que queda fuera (y los tipos desconocidos), devuelve `bool` y poda los buckets vencidos al escribir.
- `Snapshot(n)` rellena con ceros los minutos faltantes y calcula los totales. Como la poda solo ocurre en `Record`, `Snapshot` salta los buckets vencidos que aún estén en el mapa. También recorta `n` a `[1, R]` como defensa; el handler HTTP ya valida el rango.
- Los structs `Snapshot`/`MinuteCounts`/`Counts` llevan los tags JSON de la spec, así que el handler serializa el snapshot tal cual.
- Se agrega por hora del evento y no por hora de recepción: los eventos que llegan tarde caen en el minuto correcto.
- Alternativa: ring buffer de tamaño fijo indexado por minuto. Es más eficiente, pero la complejidad no se justifica con una retención de 60 buckets.

### Consumidor
Una goroutine: `for { e, err := q.Consume(ctx); ...; agg.Record(e) }`. El consumidor recibe un contexto propio, no el de las señales, para que en el apagado se pueda llamar a `q.Close()` y drenar hasta `ErrClosed`. Un solo consumidor alcanza para el volumen esperado; el mutex permite agregar más si hiciera falta.

### HTTP
- `net/http` con los patrones de Go 1.22 (`"POST /api/events"`, `"GET /api/stats"`, `"GET /{$}"`).
- `http.MaxBytesReader` (1 MiB): si se excede, 413 en vez de 400, para que el productor distinga "demasiado grande" de "mal formado". `json.Decoder.DisallowUnknownFields` desactivado (se toleran campos extra para compatibilidad con los productores); si queda algo después del primer valor JSON, se responde 400.
- `Publish` → `ErrQueueFull` o `ErrClosed` (apagado en curso) se traducen a 503; ambos significan "reintenta más tarde".
- `GET /api/stats` recibe el agregador por la interfaz `StatsSource{Snapshot, RetentionMinutes}`, limita `minutes` a `min(60, R)` para que la respuesta siempre tenga exactamente N minutos, y agrega `Cache-Control: no-store` porque la página hace polling.
- Los errores se devuelven como `{"error": "..."}` mediante un helper `writeJSON`.
- La página se sirve con `//go:embed web/index.html`: HTML + CSS + JS vanilla, `setInterval(fetch('/api/stats'), 2000)`, tabla con barras CSS proporcionales y un badge de error.
- Polling en lugar de SSE: lo eligió el usuario, no mantiene conexiones abiertas y alcanza con 2 s de latencia.

### Configuración
Flags con valores por defecto: `-addr :8080`, `-queue-size 1000`, `-retention 60m`, `-simulate false`, `-simulate-rate 5` (eventos/s), `-simulate-failure-ratio 0.2`.

### Apagado ordenado
`signal.NotifyContext` (SIGINT/SIGTERM) → `server.Shutdown(ctx 5s)` → simulador detenido → `q.Close()` → esperar a que el consumidor termine de drenar → salir.

### Logging
`log/slog` con handler de texto; en `Warn` los eventos rechazados por validación y por cola llena.

## Risks / Trade-offs

- [Los conteos se pierden al reiniciar] → Aceptado (Non-goal); la interfaz `Queue` y el agregador aislado permiten persistir o usar Redis después.
- [Relojes desincronizados entre productores y servicio] → Se rechazan eventos > 1 min en el futuro y se ignoran los que están fuera de la retención.
- [Un productor rápido llena la cola] → 503 explícito; la capacidad es configurable.
- [Endpoints sin autenticación] → Pensado para uso local/demo; documentarlo en el README.
- [Cada pestaña abierta hace polling] → Costo despreciable (el snapshot es O(60)).

## Open Questions

- Ninguna que bloquee; el soporte de Redis Streams queda para un cambio futuro.

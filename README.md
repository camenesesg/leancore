# Dashboard de pagos por minuto

Servicio en Go que consume eventos de pago (`payment.processed` y `payment.failed`) desde una cola en memoria y muestra, casi en tiempo real, cuántos pagos se procesaron y cuántos fallaron en cada minuto.

- `POST /api/events`: publica un evento en la cola.
- `GET /api/stats`: conteos por minuto en JSON.
- `GET /`: página que consulta `/api/stats` cada 2 segundos.

Solo usa la librería estándar de Go. Los conteos viven en memoria y se pierden al reiniciar el proceso.

> **Sin autenticación.** Ningún endpoint pide credenciales: cualquiera que alcance el puerto puede publicar eventos y leer las estadísticas. Está pensado para uso local o demos; no lo expongas a una red pública sin ponerle delante un proxy con autenticación.

## Requisitos

Go 1.22 o superior.

## Ejecutar

```sh
go run ./cmd/dashboard -simulate
```

Abre <http://localhost:8080>. Con `-simulate` el servicio genera 5 eventos por segundo (20 % fallidos) para que el tablero tenga datos; sin esa opción espera eventos en `POST /api/events`.

Detén el servicio con Ctrl+C: deja de aceptar peticiones, procesa los eventos que aún estén en la cola y termina.

Sin Go instalado, con Docker:

```sh
docker run --rm -it -p 8080:8080 -v "$PWD":/src -w /src golang:1.22 go run ./cmd/dashboard -simulate
```

### Opciones

| Opción | Por defecto | Descripción |
|---|---|---|
| `-addr` | `:8080` | Dirección HTTP en la que escucha. |
| `-queue-size` | `1000` | Máximo de eventos en cola. Si está llena, `POST /api/events` responde 503. |
| `-retention` | `60m` | Cuánto tiempo se guardan los conteos (minutos enteros, mínimo `1m`). También limita el máximo de `minutes` en `/api/stats`. |
| `-simulate` | `false` | Publica eventos aleatorios. |
| `-simulate-rate` | `5` | Eventos simulados por segundo (máximo efectivo: 1000). |
| `-simulate-failure-ratio` | `0.2` | Fracción de eventos simulados que fallan, entre 0 y 1. |

## API

### Publicar un evento

```sh
curl -i -X POST http://localhost:8080/api/events \
  -H 'Content-Type: application/json' \
  -d '{"type":"payment.processed","id":"p-1001","amount":49.90}'
```

```sh
curl -i -X POST http://localhost:8080/api/events \
  -H 'Content-Type: application/json' \
  -d '{"type":"payment.failed","id":"p-1002"}'
```

| Campo | Obligatorio | Descripción |
|---|---|---|
| `type` | sí | `payment.processed` o `payment.failed`. |
| `id` | sí | Identificador del pago, no vacío. |
| `amount` | no | Número mayor o igual a 0. |
| `occurred_at` | no | Hora del pago en RFC 3339. Si falta, se usa la hora de recepción. No puede estar más de 1 minuto en el futuro. |

Los campos adicionales se ignoran. El evento se cuenta en el minuto de su `occurred_at` (UTC); si ese minuto ya salió de la ventana de retención, se acepta pero no se cuenta.

| Respuesta | Cuándo |
|---|---|
| `202 Accepted` | El evento quedó en la cola. |
| `400 Bad Request` | JSON inválido o evento que no cumple las reglas de arriba. |
| `413 Payload Too Large` | Cuerpo de más de 1 MiB. |
| `503 Service Unavailable` | Cola llena o servicio apagándose; reintenta más tarde. |

Los errores tienen la forma `{"error": "mensaje"}`.

### Consultar estadísticas

```sh
curl -s 'http://localhost:8080/api/stats?minutes=5'
```

```json
{
  "generated_at": "2026-09-25T12:10:03Z",
  "minutes": [
    {"minute": "2026-09-25T12:06:00Z", "processed": 0, "failed": 0},
    {"minute": "2026-09-25T12:07:00Z", "processed": 12, "failed": 1},
    {"minute": "2026-09-25T12:08:00Z", "processed": 9, "failed": 3},
    {"minute": "2026-09-25T12:09:00Z", "processed": 14, "failed": 0},
    {"minute": "2026-09-25T12:10:00Z", "processed": 4, "failed": 1}
  ],
  "totals": {"processed": 39, "failed": 5}
}
```

`minutes` es opcional: por defecto 15, entre 1 y 60 (o la retención, si es menor). La respuesta trae exactamente esa cantidad de minutos, del más antiguo al actual, con ceros donde no hubo eventos. Un valor fuera de rango responde 400.

## Tests

```sh
go test -race ./...
go vet ./...
```

## Estructura

```
cmd/dashboard/        punto de entrada: opciones, conexión de piezas, apagado ordenado
internal/event/       modelo del evento y validación
internal/queue/       interfaz Queue y cola en memoria acotada
internal/consumer/    bucle que pasa eventos de la cola al agregador
internal/aggregator/  conteos por minuto con ventana de retención
internal/httpapi/     endpoints HTTP y página (web/index.html embebida)
internal/simulator/   generador de eventos para demos
```

La cola está detrás de la interfaz `queue.Queue`, así que se puede cambiar por una implementación con Redis sin tocar el resto.

# payment-stats-dashboard Specification

## Purpose

Agrega los eventos de pago consumidos en conteos por minuto de pagos exitosos y fallidos, y los expone mediante un endpoint JSON y una página web que se actualiza casi en tiempo real.

## Requirements

### Requirement: Per-minute aggregation
El sistema SHALL agrupar los eventos en intervalos de un minuto en UTC, según `occurred_at` truncado al minuto. Cada intervalo MUST llevar dos conteos: `processed` (eventos `payment.processed`) y `failed` (eventos `payment.failed`).

#### Scenario: Events in the same minute
- **WHEN** se consumen 3 eventos `payment.processed` y 1 `payment.failed` con `occurred_at` entre 12:05:00 y 12:05:59 UTC
- **THEN** el intervalo `12:05` tiene `processed = 3` y `failed = 1`

#### Scenario: Events in different minutes
- **WHEN** se consume un evento `payment.processed` a las 12:05:30 y otro a las 12:06:10
- **THEN** los intervalos `12:05` y `12:06` tienen cada uno `processed = 1`

### Requirement: Retention window
El sistema SHALL conservar conteos solo dentro de una ventana de retención configurable R (por defecto 60 minutos). R se expresa en minutos enteros: un valor configurado que no sea un múltiplo exacto de un minuto MUST redondearse hacia abajo, con un mínimo de 1 minuto. La ventana MUST abarcar el minuto actual y los R−1 minutos anteriores. Los intervalos más antiguos que la ventana MUST descartarse y nunca reportarse, y los eventos consumidos cuyo `occurred_at` esté fuera de la ventana MUST ignorarse sin afectar ningún conteo.

#### Scenario: Old event ignored
- **WHEN** se consume un evento con `occurred_at` de hace 2 horas y la retención es de 60 minutos
- **THEN** ningún intervalo devuelto por `GET /api/stats` cambia

#### Scenario: Oldest minute in the window is counted
- **WHEN** la retención es de 60 minutos, la hora actual es 12:00:30 UTC y se consume un evento con `occurred_at` 11:01:00 UTC
- **THEN** el evento se cuenta en el intervalo `11:01`

### Requirement: Stats JSON endpoint
El sistema SHALL exponer `GET /api/stats` con un parámetro opcional `minutes` (entero de 1 a `min(60, R)`, por defecto `min(15, R)`, donde R es la retención en minutos) y responder `200` con `Content-Type: application/json`, `Cache-Control: no-store` y este formato:

```json
{
  "generated_at": "2026-09-25T12:10:03Z",
  "minutes": [
    {"minute": "2026-09-25T11:56:00Z", "processed": 0, "failed": 0},
    {"minute": "2026-09-25T12:10:00Z", "processed": 4, "failed": 1}
  ],
  "totals": {"processed": 4, "failed": 1}
}
```

- `minutes` MUST tener exactamente N elementos, uno por minuto, del más antiguo al actual inclusive; los minutos sin eventos MUST aparecer con conteos en 0.
- `totals` MUST ser la suma de los elementos devueltos.
- Un `minutes` no numérico o fuera de rango MUST producir `400 Bad Request` con cuerpo JSON `{"error": "<mensaje>"}`.

#### Scenario: Zero-filled minutes
- **WHEN** solo hubo eventos en el minuto actual y se pide `GET /api/stats?minutes=5`
- **THEN** la respuesta tiene 5 elementos, los 4 primeros con conteos en 0

#### Scenario: Invalid minutes parameter
- **WHEN** se pide `GET /api/stats?minutes=0`
- **THEN** la respuesta es `400` con un mensaje de error

#### Scenario: Minutes bounded by retention
- **WHEN** la retención es de 10 minutos y se pide `GET /api/stats?minutes=11`
- **THEN** la respuesta es `400`; y `GET /api/stats` sin parámetro devuelve 10 elementos

### Requirement: Dashboard page
El sistema SHALL servir en `GET /` una página HTML autocontenida (sin recursos externos) que:
- consulte `GET /api/stats` cada 2 segundos;
- muestre, por minuto, los conteos de pagos exitosos y fallidos (tabla y/o barras) y los totales;
- muestre la hora de la última actualización y un indicador visible cuando la consulta falle, sin dejar de reintentar.

#### Scenario: Page refreshes automatically
- **WHEN** la página está abierta y se acepta un nuevo evento del minuto actual
- **THEN** en un máximo de ~3 segundos la página refleja el nuevo conteo sin recargar manualmente

#### Scenario: Backend unavailable
- **WHEN** una consulta a `/api/stats` falla
- **THEN** la página muestra un indicador de error y reintenta en el siguiente ciclo

## Test Cases

### TC-DASH-01: Count successes and failures in the same minute (positive)
- **Given** un reloj fijo en 12:05:30 UTC y el agregador vacío
- **When** se registran 2 `payment.processed` y 1 `payment.failed` con `occurred_at` en 12:05
- **Then** `GET /api/stats?minutes=1` devuelve el minuto `12:05` con `processed = 2`, `failed = 1` y `totals` iguales

### TC-DASH-02: Zero-fill empty minutes (positive)
- **Given** un reloj fijo en 12:10:00 UTC y un único evento `payment.failed` a las 12:08:15
- **When** se pide `GET /api/stats?minutes=3`
- **Then** la respuesta contiene los minutos `12:08`, `12:09`, `12:10` en ese orden con `failed` = 1, 0, 0

### TC-DASH-03: Reject out-of-range minutes (negative)
- **Given** el servicio en ejecución
- **When** se pide `GET /api/stats?minutes=61` y luego `GET /api/stats?minutes=abc`
- **Then** ambas respuestas son `400` con `{"error": ...}`

### TC-DASH-04: Ignore events outside retention (negative)
- **Given** retención de 60 minutos y un reloj fijo en 12:00 UTC
- **When** se registra un evento con `occurred_at` a las 10:30 UTC
- **Then** ningún conteo cambia y no se crea un intervalo para 10:30

### TC-DASH-05: Serve dashboard page (positive)
- **Given** el servicio en ejecución
- **When** se pide `GET /`
- **Then** la respuesta es `200` con `Content-Type: text/html` y el HTML referencia `/api/stats` con un intervalo de refresco de 2 segundos

### TC-DASH-06: Bound minutes by retention (negative)
- **Given** el servicio configurado con retención de 10 minutos
- **When** se pide `GET /api/stats?minutes=11` y luego `GET /api/stats` sin parámetro
- **Then** la primera respuesta es `400` con `{"error": ...}` y la segunda es `200` con 10 elementos en `minutes`

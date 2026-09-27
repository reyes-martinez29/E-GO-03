# E-GO03 — Hot Sale LATAM: productor y consumidor exactly-once

Productor que publica `eventos_hotsale.jsonl` a **NATS JetStream** y consumidor
que deduplica por `event_id` y mantiene agregados por país y tipo de evento.
Las decisiones, cifras y hallazgos están en [decisions.md](decisions.md).

## Estructura

| Ruta | Qué hace |
| :--- | :--- |
| `cmd/producer/` | Lee el JSONL en orden y lo publica con `PublishMsgAsync` (contrapresión con `-max-pending`). Publica un mensaje EOF al final. |
| `cmd/consumer/` | Pull consumer → shards por `hash(event_id)`; cada shard hace decode + dedup + agregado sin locks. Ack por lotes (`AckAll`) en orden. Mide throughput y latencia e2e, escribe reporte JSON y heap profile. |
| `cmd/verify/` | Oráculo: calcula el agregado verdadero desde el archivo (map sin ventana) y lo compara contra el reporte del consumidor. |
| `internal/dedup/` | Ventana de deduplicación: anillo de generaciones de maps con reloj inyectable. |
| `internal/event/` | Parseo del evento (UUID a `[16]byte`, dinero a centavos `int64`). |
| `internal/agg/` | Agregado por país × tipo y su reporte. |
| `internal/broker/` | Configuración compartida del stream `HOTSALE`. |
| `internal/metrics/` | Percentiles de latencia e histograma de retraso de reintentos. |
| `scripts/bench.sh` | Corrida completa reproducible (consumidor + productor + reporte). |
| `docker-compose.yml` | NATS 2.11 con JetStream en disco. |

## Requisitos

- Go 1.25+ (probado con 1.26.5)
- Docker Desktop (probado con 28.5)
- Python 3 + numpy, solo para generar el dataset

## Comandos

Todos se ejecutan desde la raíz del proyecto. Los ejemplos usan Git Bash; en
PowerShell son iguales cambiando `./bin/x.exe` por `.\bin\x.exe`.

### 1. Generar el dataset

El generador y el dataset **no están en el repositorio** (ver `.gitignore`):
`generate_events_go03.py` viene con el material del curso E-GO03 y el JSONL
pesa ~666 MB. Copia el generador a la raíz del proyecto y ejecútalo:

```bash
pip install numpy                  # si no lo tienes
python generate_events_go03.py     # escribe eventos_hotsale.jsonl en la raíz
# Eventos unicos: 3,035,188
# Duplicados inyectados: 91,562 (3.02%)
# Total de lineas en el archivo (unicos + duplicados): 3,126,750
```

El generador fija la semilla de numpy, así que conteos, países y montos
salen idénticos en cada ejecución y las cifras de `decisions.md` son
comparables. Lo único que cambia son los `event_id`/`order_id`
(`uuid.uuid4()` no usa esa semilla), y eso no afecta ningún resultado.

Para comprobar el archivo antes de levantar nada, el oráculo lo lee directo:

```bash
go run ./cmd/verify
# líneas: 3126750  únicos: 3035188  duplicados: 91562 ...
```

### 2. Levantar el broker

```bash
docker compose up -d --wait
# monitoreo: http://localhost:8222/jsz
```

### 3. Compilar y probar

```bash
go test ./...
go build -o bin/ ./cmd/...
```

#### Race detector

`go test -race` necesita cgo con un gcc de 64 bits. En Windows sin mingw-w64
falla con `cc1.exe: sorry, unimplemented: 64-bit mode not compiled in`. La
alternativa sin instalar nada es correrlo en un contenedor Linux:

```bash
MSYS_NO_PATHCONV=1 docker run --rm -v "$PWD":/src -w /src -e GOFLAGS=-buildvcs=false golang:1.26 go test -race -count=1 ./...
```

Para probar el pipeline completo bajo el race detector, productor y
consumidor se compilan con `-race` y corren en contenedores dentro de la red
de compose (`go-03_default`, el broker es `nats:4222`):

```bash
MSYS_NO_PATHCONV=1 docker run --rm -v "$PWD":/src -w /src -e GOFLAGS=-buildvcs=false golang:1.26 \
  sh -c 'go build -race -o bin/consumer-race ./cmd/consumer && go build -race -o bin/producer-race ./cmd/producer'

mkdir -p results/race
MSYS_NO_PATHCONV=1 docker run -d --name race-consumer --network go-03_default -v "$PWD":/src -w /src golang:1.26 \
  sh -c "./bin/consumer-race -url nats://nats:4222 -pprof '' -out results/race > results/race/consumer.log 2>&1"
until grep -q "esperando eventos" results/race/consumer.log; do sleep 0.5; done
MSYS_NO_PATHCONV=1 docker run --rm --network go-03_default -v "$PWD":/src -w /src golang:1.26 \
  sh -c "./bin/producer-race -url nats://nats:4222 > results/race/producer.log 2>&1"
docker wait race-consumer && docker rm race-consumer
grep -c "WARNING: DATA RACE" results/race/*.log     # esperado: 0 en ambos
```

`MSYS_NO_PATHCONV=1` evita que Git Bash reescriba `/src` como una ruta de
Windows; en PowerShell se omite.

### 4. Correr el pipeline

El consumidor tiene que estar corriendo antes de que llegue el primer evento.
Al arrancar vacía el stream (`-purge=true`, por defecto) y crea el consumer
desde la primera secuencia.

```bash
# terminal 1
./bin/consumer.exe

# terminal 2 (cuando el consumidor diga "esperando eventos...")
./bin/producer.exe                 # sin límite: throughput máximo
./bin/producer.exe -rate 20000     # carga normal (baseline de decisions.md)
```

El consumidor termina solo al recibir el EOF del productor. Imprime el
agregado, el conteo de duplicados, throughput, latencia p50/p95/p99 y 4
chequeos de consistencia, y escribe `results/consumer-<fecha>.json` y
`results/heap-<fecha>.pb.gz`.

### 5. Verificar contra la verdad del archivo

```bash
./bin/verify.exe -report results/consumer-<fecha>.json
# OK duplicados: 91562 == 91562
# OK agregado idéntico al centavo en los 5 países y el total
```

### 6. Mediciones reproducibles

```bash
scripts/bench.sh baseline-20k-full 20000      # <label> <rate> [limit]
scripts/bench.sh flood-full 0
```

### Demostración: qué pasa sin deduplicación

```bash
CONSUMER_ARGS="-no-dedup" scripts/bench.sh no-dedup-full 0
./bin/verify.exe -report results/bench/consumer-<fecha>.json   # FALLA en los 5 países
```

### 7. Profiling

Con el consumidor corriendo, pprof está en `localhost:6060`:

```bash
go tool pprof -http=: http://localhost:6060/debug/pprof/heap
go tool pprof -http=: "http://localhost:6060/debug/pprof/profile?seconds=10"
# fuga en la ventana: comparar dos snapshots del heap
go tool pprof -base heap1.pb.gz heap2.pb.gz
```

Además, el productor acepta `-cpuprofile archivo.pb.gz`.

### 8. Reinicio del consumidor (agregado no persistido)

Si el consumidor se cae, se relanza con `-purge=false`: re-consume desde el
primer mensaje retenido en el stream y reconstruye ventana y agregado juntos
(ver decisions.md §5).

```bash
./bin/consumer.exe -purge=false
```

## Flags principales

| Binario | Flag | Default | Uso |
| :--- | :--- | :--- | :--- |
| producer | `-rate` | 0 | ev/s (0 = sin límite) |
| producer | `-limit` | 0 | publicar solo N líneas |
| producer | `-max-pending` | 1024 | publicaciones sin PubAck |
| producer | `-run-id` | timestamp | prefijo del `Nats-Msg-Id` |
| consumer | `-shards` | 8 | shards de dedup + agregado |
| consumer | `-dedup-window` / `-dedup-gens` / `-dedup-min` | 5m / 10 / 3m | ventana de dedup |
| consumer | `-ack-batch` / `-ack-every` | 1000 / 10ms | lote de ack |
| consumer | `-purge` | true | vaciar el stream al arrancar |
| consumer | `-no-dedup` | false | **demostración**: desactiva la dedup para ver el agregado inflado |
| consumer | `-pprof` | localhost:6060 | vacío = desactivado |

## Apagar

```bash
docker compose down        # conserva el volumen
docker compose down -v     # borra también el stream
```

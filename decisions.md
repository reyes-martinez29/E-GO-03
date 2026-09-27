# decisions.md — E-GO03 Hot Sale LATAM

Este archivo registra las decisiones del ejercicio y los hallazgos que las
motivaron, con la evidencia de cada uno. Todas las cifras salen de corridas
reales en esta máquina (Windows 11, 20 hilos, Docker Desktop, NATS 2.11 en un
contenedor, Go 1.26.5). Los reportes JSON y los heap profiles están en
`results/`.

## Resumen

| Pregunta (§6 de la guía) | Respuesta corta |
| :--- | :--- |
| Broker | **NATS JetStream** con almacenamiento en disco. Se descartó conscientemente la atomicidad de Redis + Lua (§1). |
| Ventana de dedup | **5 min de tiempo de procesamiento, en 10 generaciones de 30 s**: garantiza ≥ 4 min 30 s de retención frente a un retraso máximo de reintento de 179 s (§2). |
| Throughput / p95 (baseline Go04) | **20,000 ev/s sostenidos con p95 e2e = 1.13 ms** (p99 4.45 ms) sobre el archivo completo. Techo medido: 66–77k ev/s sin límite de tasa (§3). |
| Duplicados detectados vs. generador | **91,562 = 91,562**. El agregado es idéntico al centavo al del oráculo `cmd/verify` en los 5 países (§4). |

---

## 1. Redis Streams vs. NATS JetStream

### Qué se comparó, con el volumen real

El archivo tiene 3,126,750 mensajes de ~213 B (666 MB).

| Criterio | Redis Streams | NATS JetStream |
| :--- | :--- | :--- |
| Dónde viven los 3.1M mensajes | En RAM. El stream (listpacks + IDs) ocupa más que los 666 MB del archivo, así que el contenedor necesita >1 GB, salvo que se recorte con `MAXLEN` y se pierda la posibilidad de re-consumir. | En disco (`-sd /data`). La RAM del broker no depende del tamaño del log. |
| Publicación masiva | `XADD` en pipeline. | `PublishAsync` con límite de mensajes en vuelo: la contrapresión viene integrada. |
| Entrega | Consumer groups, PEL, `XACK` por ID. | Pull consumer, `MaxAckPending`, `AckAll` (un ack confirma un rango). |
| Deduplicación en el broker | No tiene. | `Nats-Msg-Id` + ventana `Duplicates` (2 min por defecto). |
| Dedup + agregado + ack atómicos | **Sí**: un script Lua por lote hace `SET id NX EX 300` + `HINCRBY` + `XACK`. | No: el ack es independiente del estado del consumidor. |

### Trade-off principal

Redis puede ser broker y almacén de estado a la vez. Con un `EVALSHA` por
lote de `XREADGROUP`, la dedup, el agregado y el ack quedan en una sola
operación atómica. Eso es *effectively-once* que además **sobrevive a un
reinicio del consumidor**. JetStream es un log en disco que aguanta el volumen
sin esfuerzo, pero el ack está separado del estado: la garantía es
at-least-once del broker más un consumidor idempotente.

### Decisión: NATS JetStream

**Qué se descartó y por qué:** la atomicidad de Redis + Lua. Esta semana no se
exige persistir el agregado (§5). Pagar ~1 GB de RAM por el stream y una ida y
vuelta a Redis por lote, a cambio de una persistencia que no se usa, no se
justifica. En Go04, si se exige que el agregado sobreviva a una caída, esta
decisión se revisa primero.

**A favor de JetStream, además:**
- `PublishAsync` con `-max-pending` da contrapresión sin código propio. La RAM
  del productor queda acotada sin importar el tamaño del archivo.
- `Nats-Msg-Id` permite separar dos capas de duplicados (ver la siguiente
  sección).

### `Nats-Msg-Id`: por qué NO es el event_id

Si el productor pusiera `Nats-Msg-Id = event_id`, el broker descartaría los
reintentos del gateway antes de que lleguen al consumidor. El consumidor
contaría 0 duplicados y la verificación contra el generador sería imposible.
Además, la ventana del broker por defecto (2 min) es **menor** que el retraso
máximo de reintento (3 min): dejaría pasar una parte y la dedup sería
incompleta sin que nadie lo note.

Por eso `Nats-Msg-Id = <run-id>-<número de línea>`:

- **Reintentos del productor** (reenvío de la misma línea tras un fallo de
  PubAck): los absorbe el broker con `Duplicates = 1 min`.
- **Reintentos del gateway** (líneas distintas con el mismo `event_id`): llegan
  al consumidor, que los detecta y los cuenta.

El `run-id` fue un hallazgo; se explica en el hallazgo H4 de la bitácora.

---

## 2. Ventana de deduplicación

### Qué recuerda y con qué reloj

Recuerda los `event_id` ya aplicados durante **5 minutos de tiempo de
procesamiento** (reloj de pared del consumidor), no del campo `timestamp` del
evento.

**Por qué no el `timestamp` del evento:** el archivo **no está ordenado por
tiempo de evento**. Las primeras tres líneas son 18:43, 05:32 y 01:28; el
generador reparte cada orden al azar en una ventana de 20 h dentro de cada
bloque. Una ventana que expulse llaves con `ts < max_ts_visto − 3 min` ve
enseguida un `max_ts` cercano a las 20 h. Desde ese momento expulsa casi todo
lo que entra y deja pasar duplicados. Es la trampa principal del dataset (H1).

**Por qué el tiempo de procesamiento es el correcto:** los "3 minutos" son el
tiempo que tarda el gateway en reenviar. En producción, esa demora se mide en
el reloj de quien recibe. El consumidor procesa en orden de llegada, así que
el reintento de un evento aplicado hace *t* segundos llega ≤ 3 min después.

### Tamaño: 5 minutos, 10 generaciones de 30 s

- El generador usa `retry_delay = rng.integers(2, 180)`: el retraso máximo
  posible es **179 s**. Medido en el consumidor: min 2 s, max 179 s, media
  90.4 s, 0 fuera de rango (§4).
- La ventana es un anillo de 10 maps. Una llave vive como mínimo
  (G−1)/G × 5 min = **4 min 30 s**. Eso da 1 min 30 s de margen sobre 3 min
  para backlog en la cola (el original quedó encolado y su reintento llegó
  rápido) y para desfase de reloj.
- `dedup.New` **rechaza** cualquier configuración cuya retención garantizada
  sea menor a `-dedup-min` (3 min). Por ejemplo, 3 min en 10 generaciones solo
  garantiza 2 min 42 s y se rechaza (`TestInvalidConfig`).

### Estructura de datos

```go
type Window struct {
    gens []generation // anillo; generation{ m map[[16]byte]uint32; start time.Time }
    cur  int
    ...
}
```

- **Llave `[16]byte`**: el UUID de 36 caracteres se decodifica a binario. Es la
  mitad de memoria que el string y no aloca al buscar. Benchmark: 204 ns/op,
  **0 allocs/op** (incluye el crecimiento del map).
- **Valor `uint32`**: el `timestamp` del original. Permite medir el retraso de
  cada reintento detectado, que es la evidencia de que se detectan reintentos
  reales y no colisiones.
- **Búsqueda**: recorre las 10 generaciones de la más nueva a la más vieja:
  O(G) con G constante, o sea O(1).
- **Expiración**: se descarta **la generación completa** (se reemplaza el map
  por uno nuevo), sin `delete` llave por llave. Los maps de Go **no devuelven
  memoria al hacer `delete`**: un map único con expiración por llave conserva
  sus buckets en el pico histórico y parece una fuga. Tirar el map entero lo
  libera todo para el GC.
- **Reloj inyectable** (`Config.Now`): las pruebas mueven el tiempo sin
  dormir.

### Protección de memoria

`-dedup-max-per-gen` pone un tope de llaves por generación. Si el tope obliga
a rotar antes de tiempo y se expulsan llaves con menos de 3 min, se incrementa
`premature_evictions`. En ese caso la garantía se rompió **y queda
registrado**. El chequeo `no_premature_evictions` del reporte lo vigila. En
todas las corridas: **0**.

### Memoria medida

Heap profile al final de la corrida completa (3,035,188 llaves vivas):
`dedup.(*Window)` ocupa **~156 MB**, o sea **~51 B por llave** (16 B de llave,
4 B de valor, más overhead del map). El reporte también registra el heap pico.

**Proyección para Go04:** en estado estable la ventana contiene
≈ tasa × 5 min llaves.

| Tasa sostenida | Llaves en ventana | Memoria (~51 B/llave) |
| :--- | :--- | :--- |
| 20k ev/s | 6.0 M | ~306 MB |
| 77k ev/s (saturación medida) | 23.1 M | ~1.2 GB |
| 200k ev/s (10× el baseline) | 60 M | **~3.1 GB** |

A 10× la ventana en memoria de un solo proceso deja de ser trivial. Eso es un
input directo para Go04: particionar entre varios consumidores por
`hash(event_id)` o mover la dedup a un almacén externo.

### Pruebas (`internal/dedup/window_test.go`)

| Prueba | Qué garantiza |
| :--- | :--- |
| `TestRetryAtWorstCaseDelayIsDetected` | Un reintento a 179 s se detecta sin importar en qué punto de la generación entró el original. |
| `TestGuaranteedRetentionBoundary` | Una llave vive exactamente la retención garantizada (4 min 30 s) y expira tras la ventana completa. |
| `TestSizeIsBoundedUnderSteadyTraffic` | Con 1 h simulada de tráfico constante, el tamaño nunca supera tasa × ventana (detector de fuga). |
| `TestLongIdleExpiresEverything` | Tras 24 h sin tráfico todo expira, sin falsas expulsiones prematuras. |
| `TestMaxPerGenCountsPrematureEvictions` | El tope de memoria contabiliza lo que expulsa antes de tiempo. |

### Límite honesto del dataset

En el archivo, **cada reintento está en la línea inmediatamente siguiente a su
original** (distancia máxima medida: 1 línea, con `cmd/verify`). El retraso de
2 s a 3 min solo existe en el campo `timestamp`. Al hacer replay, el reintento
llega microsegundos después del original, así que la corrida completa **no
ejercita la dimensión temporal de la ventana**. Eso lo cubren las pruebas con
reloj falso de la tabla anterior y el experimento de ventana corta del
hallazgo H6.

---

## 3. Throughput y latencia (baseline para Go04)

### Metodología

- **Latencia end-to-end**: desde `Pub-Ts` hasta que el shard **aplicó** el
  evento al agregado (o lo descartó como duplicado). `Pub-Ts` es un header en
  UnixNano que el productor pone justo antes de `PublishMsgAsync`. Productor y
  consumidor corren en la misma máquina, así que comparten reloj. Se registra
  la latencia de cada mensaje de primera entrega (3,126,750 muestras en un
  `[]int64`, ~25 MB). Al final se ordenan y se sacan p50/p95/p99/max
  **exactos**, sin aproximación de histograma.
- **Throughput del consumidor**: primeras entregas ÷ (último evento aplicado −
  primer mensaje recibido).
- **Throughput end-to-end**: primeras entregas ÷ (último evento aplicado −
  primer `Pub-Ts`).
- **"Carga normal"**: el productor publica a una **tasa fija** (`-rate`),
  programada contra el reloj absoluto para que los retrasos no se acumulen.
  Publicar todo el archivo de golpe no es carga normal: la "latencia" que se
  mide así es el tiempo de espera en el backlog (en la v1, p95 = 25.6 s, ver
  H3).
- **Corrida reproducible**: `scripts/bench.sh <label> <rate> [limit]`. El
  consumidor purga el stream y crea un consumer nuevo en cada corrida.

### Resultados

Barrido de tasa fija (1M líneas por corrida, `scripts/bench.sh`, reportes en
`results/bench/`):

| Tasa objetivo | Throughput consumidor | p50 | **p95** | p99 | max | Heap pico |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: |
| 5,000 ev/s | 5,000 ev/s | 0.54 ms | 0.64 ms | 1.07 ms | 168.9 ms | 92 MiB |
| 20,000 ev/s | 20,000 ev/s | 0.57 ms | 1.12 ms | 2.08 ms | 15.7 ms | 99 MiB |
| 40,000 ev/s | 40,002 ev/s | 1.05 ms | 2.44 ms | 10.53 ms | 33.7 ms | 73 MiB |
| 60,000 ev/s | 60,001 ev/s | 1.57 ms | 10.49 ms | 22.75 ms | 58.3 ms | 67 MiB |

Archivo completo (3,126,750 líneas):

| Corrida | Throughput e2e | p50 | **p95** | p99 | max | Dups | Verificado vs. oráculo |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| **Baseline: 20k ev/s** | 20,000 ev/s (156.3 s) | 0.58 ms | **1.13 ms** | 4.45 ms | 55.4 ms | 91,562 | OK al centavo |
| Sin límite de tasa | 66,108 ev/s (47.3 s) | 4.00 ms | 16.36 ms | 30.30 ms | 189.1 ms | 91,562 | OK al centavo |

Notas:
- **El throughput máximo varía entre corridas** (66k en esta, 76.6k en la
  corrida de H3, 77k en el barrido de H5). El broker corre en Docker Desktop
  sobre WSL2 y comparte la máquina con productor y consumidor. Para Go04 se
  toma el techo como un **rango, 66–77k ev/s**, no como un número exacto.
- A tasas fijas el consumidor iguala exactamente la tasa del productor: el
  consumidor nunca fue el cuello de botella después de H3.
- El max de 169 ms a 5k ev/s es un solo evento atípico (p99 = 1.07 ms). No se
  investigó más; lo más probable es una pausa del scheduler o del GC en una
  máquina compartida.

### Por qué 20,000 ev/s como "carga normal"

- La latencia está **plana hasta ~20k ev/s** (p95 de 0.64 → 1.12 ms desde
  5k) y el **codo aparece entre 40k y 60k** (p95 2.4 → 10.5 ms, p99 10.5 →
  22.8 ms): ahí empieza a formarse cola en el broker.
- 20k ev/s es ~26–30 % del techo medido. Ese es el margen que se quiere en
  operación normal para absorber ráfagas sin salir de la zona plana.
- La tasa real del dataset es mucho menor: 3.1M eventos en 20 h ≈ **43
  ev/s**. El baseline la supera ~460 veces, así que el sistema aguanta el Hot
  Sale "tal cual" con mucha holgura. El 10× que pide Go04 (200k ev/s) está
  **por encima del techo actual** de un solo broker en esta máquina, y eso es
  justamente lo que Go04 tiene que resolver.

---

## 4. Duplicados detectados vs. conteo real del generador

| Fuente | Duplicados | Únicos | order_placed | payment_confirmed | inventory_reserved |
| :--- | ---: | ---: | ---: | ---: | ---: |
| `generate_events_go03.py` | 91,562 | 3,035,188 | 1,200,000 | 1,019,598 | 815,590 |
| `cmd/verify` (map sin ventana sobre el archivo) | 91,562 | 3,035,188 | 1,200,000 | 1,019,598 | 815,590 |
| `cmd/consumer` (pipeline completo) | **91,562** | **3,035,188** | 1,200,000 | 1,019,598 | 815,590 |

**Coinciden exactamente.** `verify -report` compara además el agregado
completo contra el del oráculo: **idéntico al centavo** en los 5 países.

Agregado final (USD calculado en centavos `int64`, sin `float64`):

| País | Órdenes | Órdenes USD | Pagos confirmados | Pagos USD | Inventario reservado |
| :--- | ---: | ---: | ---: | ---: | ---: |
| MX | 240,412 | 14,825,927.68 | 204,568 | 12,609,231.78 | 163,894 |
| AR | 240,836 | 14,845,476.11 | 204,495 | 12,612,236.36 | 163,768 |
| CO | 238,820 | 14,722,007.35 | 203,176 | 12,540,964.70 | 162,489 |
| CL | 239,995 | 14,771,085.04 | 203,717 | 12,523,200.88 | 162,838 |
| PE | 239,937 | 14,808,124.43 | 203,642 | 12,559,792.34 | 162,601 |
| **Total** | **1,200,000** | **73,972,620.61** | **1,019,598** | **62,845,426.06** | **815,590** |

**Evidencia de que son reintentos reales:** para cada duplicado, el consumidor
calcula `ts_reintento − ts_original`. Resultado: min 2 s, max 179 s, media
90.4 s, **0 fuera de [2, 180) s**, justo la distribución uniforme
`rng.integers(2, 180)` del generador.

### Contraprueba: el mismo pipeline sin deduplicación

Para mostrar qué se evita, el consumidor tiene un flag de demostración,
`-no-dedup`, que salta la ventana y cuenta cada línea como un evento. Corrida
del archivo completo (`CONSUMER_ARGS="-no-dedup" scripts/bench.sh
no-dedup-full 0`, reporte `results/bench/consumer-20260927-171132.json`):

| | Con dedup (correcto) | Sin dedup | Error |
| :--- | ---: | ---: | ---: |
| Eventos contados | 3,035,188 | 3,126,750 | +91,562 |
| Órdenes | 1,200,000 | 1,235,939 | +35,939 (+3.0 %) |
| Órdenes USD | 73,972,620.61 | 76,184,636.11 | +2,212,015.50 |
| Pagos confirmados | 1,019,598 | 1,050,464 | +30,866 |
| **Pagos USD** | **62,845,426.06** | **64,740,965.16** | **+1,895,539.10** |
| Inventario reservado | 815,590 | 840,347 | +24,757 |

`cmd/verify -report` marca **FALLA** en los 5 países y en el total.

Esto es el problema de negocio de la guía en números. Sin dedup, operaciones
vería **USD 1.9 M de pagos que no existen** y 24,757 reservas de inventario
de más. Decidir "reforzar inventario en MX" con 168,937 reservas en lugar de
las 163,894 reales es decidir sobre un número falso. Lo notable es que la
corrida sin dedup **pasa sus propios chequeos de contabilidad** (cada línea
se aplicó una vez y no se perdió nada). Solo la comparación contra la verdad
del archivo lo delata: "procesé el archivo" no es lo mismo que "procesé cada
evento exactamente una vez".

### Cuándo NO coincidirían, y cómo se distinguiría

El consumidor separa tres causas de "ver un event_id otra vez":

1. **Reintento del gateway** (`duplicates_discarded`): primera entrega del
   broker, event_id ya visto. Es lo que se compara contra el generador.
2. **Re-entrega del broker** (`broker_redeliveries`): `NumDelivered > 1`,
   porque el ack no llegó antes del `AckWait`. Se cuenta aparte para no inflar
   el número (1). Si la entrega original nunca llegó a aplicarse, la
   re-entrega sí se aplica.
3. **Reintento del productor**: lo absorbe el broker por `Nats-Msg-Id` y el
   productor lo reporta como "descartados por broker".

Además, cuatro chequeos automáticos en cada reporte cuadran las cuentas:
`first_deliveries == expected_lines` (sin pérdida),
`unique + duplicates + invalid == first_deliveries`,
`aggregate_total == unique_applied` y `no_premature_evictions`. Estos chequeos
detectaron el bug del hallazgo H4.

---

## 5. Otras decisiones

### El agregado NO se persiste (decisión explícita)

El agregado y la ventana viven solo en memoria. Para que eso sea consistente
y no un descuido:

- El stream usa retención `Limits` (no `WorkQueue`): los mensajes sobreviven
  al ack.
- En cada arranque el consumidor **borra y recrea** su consumer durable con
  `DeliverAll`. Tras una caída, `consumer -purge=false` re-consume desde el
  primer mensaje retenido y **reconstruye ventana y agregado juntos**. No hay
  estado parcial que pueda desincronizarse: se pierden o se reconstruyen los
  dos a la vez.
- **Verificado:** `consumer -purge=false` sobre el stream retenido
  re-consumió 3,126,750 mensajes y reconstruyó 3,035,188 únicos y 91,562
  duplicados, con los 4 chequeos en OK
  (`results/bench/consumer-20260927-160732.json`).
- Costo aceptado: el tiempo de recuperación es re-procesar todo el stream
  (~40 s para el archivo completo). En Go04 esto deja de ser aceptable si el
  stream crece sin límite; ahí entran snapshots del agregado + ventana junto
  con la secuencia del stream.

### Concurrencia del consumidor: shards dueños de su estado, sin `RWMutex`

```
lector it.Next() ─▶ fetcher ─▶ shard[hash(event_id) % 8] ─▶ merge final
                       │        (json decode + dedup + agregado)
                       └──▶ lotes de ack ─▶ committer (AckAll en orden)
```

- Cada evento **escribe** (dedup + agregado). Un `RWMutex` no aporta nada en
  una carga 100 % de escritura, y el "¿lo vi? → márcalo → súmalo" tendría que
  ir bajo el mismo lock, lo que serializa todo.
- Con sharding por `event_id`, un mismo id siempre cae en el mismo shard, así
  que la secuencia es atómica **por construcción**. Cada shard tiene su propia
  `Window` y su propio `Agg` (arrays `[país][tipo]` indexados, sin maps de
  strings) y al final se suman. Cero locks en el camino caliente.
- El fetcher extrae solo el `event_id` de los bytes crudos para enrutar, y el
  `json.Unmarshal` ocurre dentro del shard. Eso preserva el orden por llave
  (H2) y mantiene el decode en paralelo (8 shards).
- **Verificado con el race detector: 0 data races.** En Windows no se puede
  usar `-race`: el gcc instalado (`C:\MinGW`) es de 32 bits y el race detector
  necesita cgo de 64 bits. Por eso se corrió en un contenedor Linux
  (`golang:1.26`, comandos en el README):
  - `go test -race ./...`: OK.
  - **Pipeline completo** con productor y consumidor compilados con `-race`,
    en contenedores dentro de la red de compose, sobre las **3,126,750
    líneas**: `WARNING: DATA RACE` aparece **0 veces** en ambos logs.
    Resultado: 91,562 duplicados y los 4 chequeos en OK, a 23.8k ev/s (el
    race detector hace más lento el pipeline). Evidencia en
    `results/race/full/`, con una corrida previa de 300k líneas en
    `results/race/`.

  Esto cubre las interacciones concurrentes reales: lector ↔ fetcher, fetcher
  ↔ shards, contadores `pending` de los lotes ↔ committer, contadores
  atómicos ↔ monitor, y en el productor el lazo principal ↔ `confirmAcks`.
- Dinero en **centavos `int64`**, parseados del texto decimal sin pasar por
  `float64`. Sumar ~1M floats acumula error y el total no cuadraría al
  centavo con el oráculo.

### Productor: un lector secuencial, sin worker pool

La guía exige publicar **en el orden del archivo**. Un pool de workers con
`json.Unmarshal` desordena y obliga a reensamblar. Además, el productor no
necesita parsear: publica los bytes crudos de la línea. Lo que sí es
obligatorio es `bytes.Clone(scanner.Bytes())`, porque el `Scanner` reutiliza
su buffer y el `PubAckFuture` retiene el mensaje para reintentarlo. El perfil
de CPU lo confirmó: el productor usa ~1.5 cores y la mayor parte se va en
syscalls de red, no en CPU propio.

---

## 6. Bitácora de hallazgos

### H1: los duplicados son adyacentes y el `timestamp` no está ordenado

- **Evidencia:** un awk sobre el archivo y luego `cmd/verify`: 91,562
  duplicados, distancia máxima original → reintento = **1 línea**. Primeras
  líneas: 18:43, 05:32, 01:28.
- **Consecuencia:** se descartó una ventana por tiempo de evento (§2). Se
  documentó que el replay no ejercita la ventana temporal y se cubrió con
  pruebas de reloj falso.

### H2: el pool de decoders en paralelo rompía el orden por event_id

- **Síntoma:** la primera prueba de humo (100k líneas) reportó retrasos de
  reintento **negativos** (min −179 s, 711 fuera de rango). El generador solo
  produce retrasos positivos.
- **Causa:** el diseño inicial era fetcher → 8 decoders → shards. Dos líneas
  consecutivas con el mismo event_id las tomaban decoders distintos y llegaban
  al shard en cualquier orden: el reintento se aplicaba y el original se
  descartaba.
- **Impacto:** el conteo seguía correcto (las dos copias solo difieren en
  `timestamp`), pero se violaba el orden por llave. Cualquier lógica que
  dependa del orden (p. ej. el primer `timestamp` visto) quedaba mal.
- **Fix:** el fetcher extrae el `event_id` de los bytes crudos
  (`event.ExtractID`) y enruta; el decode se hace en el shard, que es FIFO.
  Después del fix: min 2 s, max 179 s, 0 fuera de rango.

### H3: ack por mensaje = 40 % del CPU en syscalls

- **Síntoma:** v1 con `AckExplicit` y `msg.Ack()` por evento: el consumidor
  procesaba **46,013 ev/s**, más lento que el productor (75.6k), y la p95
  "e2e" era de **25.6 s** (backlog).
- **Perfil de CPU:** `runtime.cgocall` al 38 %, todo bajo
  `nats.(*Conn).flusher → net.(*conn).Write → syscall.WSASend`. Cada ack es un
  PUB pequeño y una syscall en Windows.
- **Fix:** `AckAllPolicy` con commit ordenado (`cmd/consumer/commit.go`). El
  fetcher agrupa en lotes (1000 mensajes o 10 ms). Un solo goroutine espera a
  que cada lote, **y todos los anteriores**, estén aplicados, y solo entonces
  hace ack de su último mensaje. Así el piso de ack nunca pasa por delante de
  un evento sin aplicar, aunque los shards terminen en desorden entre sí.
- **Resultado:**

| | v1 (ack por mensaje) | v2 (AckAll por lote) |
| :--- | ---: | ---: |
| Acks enviados | 3,126,750 | **4,650** |
| Throughput del consumidor | 46,013 ev/s | **76,632 ev/s** |
| p95 e2e (productor sin límite) | 25,602 ms | **63 ms** |

### H4: `Nats-Msg-Id = número de línea` descartaba corridas completas

- **Síntoma:** en un barrido de parámetros, dos corridas seguidas: la segunda
  reportó `first_deliveries = 0` con `expected_lines = 1,500,000`. El chequeo
  `no_loss` falló.
- **Causa:** la dedup del **broker**. La segunda corrida publicó los mismos
  Msg-Id ("1"…"1500000") dentro de la ventana `Duplicates` (1 min). JetStream
  los descartó todos; solo pasó el EOF, que no lleva Msg-Id. **Purgar el
  stream no limpia la tabla de Msg-Id.**
- **Reproducción:** dos corridas de 50k líneas seguidas. La segunda mostró
  "descartados por broker: 50000" y 0 entregas.
- **Fix:** `Nats-Msg-Id = <run-id>-<línea>`, con un `run-id` por ejecución.
  Tras el fix, dos corridas seguidas entregaron 50,000 cada una. El productor
  ahora avisa si el broker descarta mensajes sin que haya habido reintentos.
- **Lección para Go04:** la dedup del broker es por *identidad de mensaje*, no
  por *identidad de negocio*, y su estado vive fuera del stream.

### H5: más mensajes en vuelo no sube el throughput, sube la latencia (y provoca 429)

Barrido de `-max-pending` en el productor (1.5M líneas, sin límite de tasa):

| max-pending | Throughput | p95 e2e | p99 e2e | Notas |
| :--- | ---: | ---: | ---: | :--- |
| 1024 | 76,398 ev/s | **14.1 ms** | 18.8 ms | |
| 4096 | 77,159 ev/s | 47.4 ms | 64.6 ms | |
| 16384 | 64,725 ev/s | 248.7 ms | 506.3 ms | JetStream respondió **429 `too many requests`** (err_code 10167) a 7,950 publicaciones |

- El throughput se estanca en ~77k ev/s: el techo es el broker (NATS en Docker
  Desktop sobre WSL2), no el cliente.
- La latencia crece con los mensajes en vuelo como predice la **ley de
  Little**: 4096 en vuelo ÷ 77k ev/s ≈ 53 ms de cola.
- Con 16384 el servidor rechaza con 429. Los **7,950 rechazos se reintentaron
  todos, con 0 pérdidas y la dedup exacta**: la ruta de reintento del
  productor quedó probada con fallos reales, no simulados.
- **Decisión:** default `-max-pending=1024`.

### H6: ventana corta en vivo: la memoria se acota y el conteo se mantiene

- **Objetivo:** ver la expulsión funcionando en una corrida real, ya que con
  la ventana de 5 min el archivo completo entra antes de la primera
  expulsión (156 s < 270 s).
- **Configuración:** `-dedup-window 20s -dedup-gens 10 -dedup-min 15s`
  (retención garantizada 18 s), archivo completo a 20k ev/s. Es un
  experimento: **no** es una configuración válida para producción, porque
  18 s < 179 s de retraso máximo.
- **Resultado:**
  - La ventana se estabilizó en **~365k llaves** (≈ 20k ev/s × 18 s), frente
    a 3.04M con la ventana de 5 min. **624 rotaciones**, 0 expulsiones
    prematuras.
  - Heap pico: **81 MiB** frente a 287 MiB con la ventana de 5 min.
  - Duplicados: **91,562**, y el agregado es idéntico al centavo según
    `verify`.
- **Lectura:** la memoria queda acotada por tasa × ventana, como predice §2.
  El conteo no cambió porque en el replay los reintentos llegan a 1 línea de
  distancia. Esto refuerza el "límite honesto" de §2: el dataset valida la
  **exactitud** de la dedup, y las pruebas con reloj falso validan la
  **ventana temporal**.

---

## 7. Qué hereda Go04

- **Baseline**: 20,000 ev/s sostenidos, p95 e2e = 1.13 ms, p99 = 4.45 ms, archivo completo sin pérdidas y con dedup exacta (`results/bench/consumer-20260927-160251.json`).
- **Techo actual**: ~77k ev/s, limitado por el broker en un solo nodo con el
  cliente en Windows. 10× el baseline no cabe en esta topología sin escalar el
  broker o particionar el stream.
- **Memoria de la ventana**: ~51 B/llave × tasa × 5 min. A 200k ev/s son
  ~3 GB en un proceso: particionar la dedup por `hash(event_id)` entre
  consumidores (el diseño por shards ya lo permite) o externalizarla.
- **Persistencia**: el agregado no sobrevive a una caída sin re-procesar todo
  el stream. Revisar snapshots (agregado + ventana + secuencia) o la opción
  Redis + Lua descartada en §1.

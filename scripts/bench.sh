#!/usr/bin/env bash
# Corre una medición completa: arranca el consumidor (purga el stream), espera
# a que esté listo, publica con el productor y espera el reporte.
#
# Uso: scripts/bench.sh <label> <rate ev/s, 0 = sin límite> [limit líneas, 0 = todas]
# Flags extra del consumidor: CONSUMER_ARGS="-no-dedup" scripts/bench.sh ...
set -euo pipefail
cd "$(dirname "$0")/.."

label=${1:?label}
rate=${2:-0}
limit=${3:-0}
out=${OUT:-results/bench}
mkdir -p "$out"
ext=""; [[ "${OS:-}" == "Windows_NT" ]] && ext=".exe"

log="$out/$label-consumer.log"
./bin/consumer$ext -label "$label" -out "$out" -pprof "" ${CONSUMER_ARGS:-} >"$log" 2>&1 &
cpid=$!
until grep -q "esperando eventos" "$log"; do sleep 0.2; done

./bin/producer$ext -rate "$rate" -limit "$limit" >"$out/$label-producer.log" 2>&1
wait "$cpid"

echo "== $label (rate=$rate limit=$limit)"
grep -E "throughput publicación" "$out/$label-producer.log"
grep -E "primeras entregas|duplicados descartados|throughput consumidor|latencia e2e|heap pico|FALLA" "$log" || true
grep -E "^reporte:" "$log"

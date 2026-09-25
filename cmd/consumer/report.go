package main

import (
	"fmt"
	"runtime"
	"time"

	"hotsale/internal/agg"
	"hotsale/internal/metrics"
)

type counts struct {
	ExpectedLines   int64 `json:"expected_lines"`   // según el EOF del productor
	Received        int64 `json:"received"`         // todas las entregas de eventos
	FirstDeliveries int64 `json:"first_deliveries"` // NumDelivered == 1
	UniqueApplied   int64 `json:"unique_applied"`
	DuplicatesSrc   int64 `json:"duplicates_discarded"` // reintentos del gateway detectados
	Redelivered     int64 `json:"broker_redeliveries"`
	RedeliveredDup  int64 `json:"broker_redeliveries_discarded"`
	Invalid         int64 `json:"invalid"`
	Acks            int64 `json:"acks_sent"` // con AckAll: uno por lote
}

type report struct {
	Label       string              `json:"label"`
	Timestamp   time.Time           `json:"timestamp"`
	Config      config              `json:"config"`
	Counts      counts              `json:"counts"`
	Checks      map[string]bool     `json:"checks"`
	Throughput  throughput          `json:"throughput"`
	Latency     metrics.Latency     `json:"latency_end_to_end"`
	RetryDelays metrics.RetryDelays `json:"retry_delays"`
	Dedup       dedupReport         `json:"dedup"`
	Memory      memory              `json:"memory"`
	Aggregate   agg.Report          `json:"aggregate"`
	HeapProfile string              `json:"heap_profile"`
}

type throughput struct {
	ConsumerSec   float64 `json:"consumer_seconds"` // primer mensaje recibido -> último aplicado
	ConsumerEvSec float64 `json:"consumer_events_per_sec"`
	EndToEndSec   float64 `json:"end_to_end_seconds"` // primer Pub-Ts -> último aplicado
	EndToEndEvSec float64 `json:"end_to_end_events_per_sec"`
}

type dedupReport struct {
	GuaranteedRetention string `json:"guaranteed_retention"`
	FinalKeys           int    `json:"final_keys"`
	Rotations           uint64 `json:"rotations"`
	PrematureEvictions  uint64 `json:"premature_evictions"`
}

type memory struct {
	PeakHeapInuseMiB  uint64 `json:"peak_heap_inuse_mib"`
	FinalHeapInuseMiB uint64 `json:"final_heap_inuse_mib_after_gc"`
}

func buildReport(cfg config, shards []*shard, firstRecv time.Time, received, invalid, totalLines int64, peakHeap uint64) report {
	var total agg.Agg
	var c counts
	var lat []int64
	var delays metrics.RetryDelays
	var d dedupReport
	minPub, lastApply := int64(1<<63-1), int64(0)
	for _, s := range shards {
		total.Merge(&s.agg)
		c.UniqueApplied += s.applied
		c.DuplicatesSrc += s.dups
		c.Redelivered += s.redelivered
		c.RedeliveredDup += s.redeliveredDup
		lat = append(lat, s.lat...)
		s.lat = nil
		delays.Merge(s.delays)
		st := s.win.Stats()
		d.FinalKeys += st.Len
		d.Rotations += st.Rotations
		d.PrematureEvictions += st.Premature
		d.GuaranteedRetention = s.win.GuaranteedRetention().String()
		minPub = min(minPub, s.minPubTs)
		lastApply = max(lastApply, s.lastApply)
	}
	delays.Finish()
	c.ExpectedLines = totalLines
	c.Received = received
	c.Invalid = invalid
	c.FirstDeliveries = received - c.Redelivered
	aggRep := total.Report()

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	r := report{
		Label: cfg.Label, Timestamp: time.Now(), Config: cfg, Counts: c,
		Latency: metrics.Summarize(lat), RetryDelays: delays, Dedup: d, Aggregate: aggRep,
		Memory: memory{PeakHeapInuseMiB: peakHeap >> 20, FinalHeapInuseMiB: ms.HeapInuse >> 20},
	}
	if lastApply > 0 {
		r.Throughput.ConsumerSec = time.Duration(lastApply - firstRecv.UnixNano()).Seconds()
		r.Throughput.ConsumerEvSec = float64(c.FirstDeliveries) / r.Throughput.ConsumerSec
		r.Throughput.EndToEndSec = time.Duration(lastApply - minPub).Seconds()
		r.Throughput.EndToEndEvSec = float64(c.FirstDeliveries) / r.Throughput.EndToEndSec
	}
	r.Checks = map[string]bool{
		// Ninguna línea publicada se perdió.
		"no_loss: first_deliveries == expected_lines": c.FirstDeliveries == totalLines,
		// Cada entrega original terminó aplicada o descartada como duplicado.
		"accounting: unique + duplicates + invalid == first_deliveries": c.UniqueApplied+c.DuplicatesSrc+c.Invalid == c.FirstDeliveries,
		// El agregado cuenta exactamente los únicos aplicados.
		"aggregate_total == unique_applied": aggRep.Total.UniqueEventsCounted == c.UniqueApplied,
		"no_premature_evictions":            d.PrematureEvictions == 0,
	}
	return r
}

func printReport(r report) {
	c := r.Counts
	fmt.Printf("\n=== CONSUMIDOR: AGREGADO FINAL ===\n")
	fmt.Printf("%-6s %10s %16s %12s %16s %12s\n", "país", "órdenes", "órdenes USD", "pagos", "pagos USD", "inventario")
	for _, cn := range []string{"MX", "AR", "CO", "CL", "PE"} {
		l := r.Aggregate.ByCountry[cn]
		fmt.Printf("%-6s %10d %16s %12d %16s %12d\n", cn, l.Orders, l.OrdersUSD, l.PaymentsConfirmed, l.PaymentsUSD, l.InventoryReserved)
	}
	t := r.Aggregate.Total
	fmt.Printf("%-6s %10d %16s %12d %16s %12d\n", "TOTAL", t.Orders, t.OrdersUSD, t.PaymentsConfirmed, t.PaymentsUSD, t.InventoryReserved)

	fmt.Printf("\n=== CONTEO ===\n")
	fmt.Printf("líneas esperadas (EOF):     %d\n", c.ExpectedLines)
	fmt.Printf("primeras entregas:          %d\n", c.FirstDeliveries)
	fmt.Printf("eventos únicos aplicados:   %d\n", c.UniqueApplied)
	fmt.Printf("duplicados descartados:     %d\n", c.DuplicatesSrc)
	fmt.Printf("re-entregas del broker:     %d (descartadas por la ventana: %d)\n", c.Redelivered, c.RedeliveredDup)
	fmt.Printf("inválidos:                  %d\n", c.Invalid)
	fmt.Printf("acks enviados (AckAll):     %d\n", c.Acks)
	fmt.Printf("retraso de reintentos:      min=%ds max=%ds media=%.1fs fuera de [2,180)s=%d\n",
		r.RetryDelays.MinSec, r.RetryDelays.MaxSec, r.RetryDelays.MeanSec, r.RetryDelays.OutOfRange)

	fmt.Printf("\n=== RENDIMIENTO ===\n")
	fmt.Printf("throughput consumidor:      %.0f ev/s (%.2fs, primer recibido -> último aplicado)\n", r.Throughput.ConsumerEvSec, r.Throughput.ConsumerSec)
	fmt.Printf("throughput end-to-end:      %.0f ev/s (%.2fs, primer publicado -> último aplicado)\n", r.Throughput.EndToEndEvSec, r.Throughput.EndToEndSec)
	l := r.Latency
	fmt.Printf("latencia e2e (publicado -> aplicado): p50=%.2fms p95=%.2fms p99=%.2fms max=%.2fms (n=%d)\n", l.P50Ms, l.P95Ms, l.P99Ms, l.MaxMs, l.Samples)
	fmt.Printf("heap pico: %d MiB; ventana final: %d llaves, %d expulsiones prematuras\n", r.Memory.PeakHeapInuseMiB, r.Dedup.FinalKeys, r.Dedup.PrematureEvictions)

	fmt.Printf("\n=== CHEQUEOS ===\n")
	for k, ok := range r.Checks {
		fmt.Printf("[%s] %s\n", map[bool]string{true: "OK", false: "FALLA"}[ok], k)
	}
}

// Package metrics: percentiles de latencia e histograma de retraso de
// reintentos. Las latencias se guardan crudas ([]int64 de nanosegundos, ~25 MB
// para 3.1M eventos) y se ordenan al final: percentiles exactos, sin
// aproximaciones de histograma.
package metrics

import (
	"slices"
	"time"
)

type Latency struct {
	Samples int64   `json:"samples"`
	P50Ms   float64 `json:"p50_ms"`
	P95Ms   float64 `json:"p95_ms"`
	P99Ms   float64 `json:"p99_ms"`
	MaxMs   float64 `json:"max_ms"`
	MeanMs  float64 `json:"mean_ms"`
}

// Summarize ordena samples (in-place) y calcula percentiles por rango más cercano.
func Summarize(samples []int64) Latency {
	if len(samples) == 0 {
		return Latency{}
	}
	slices.Sort(samples)
	pct := func(p float64) float64 {
		i := int(p*float64(len(samples))+0.5) - 1
		i = max(0, min(i, len(samples)-1))
		return ms(samples[i])
	}
	var sum float64
	for _, s := range samples {
		sum += float64(s)
	}
	return Latency{
		Samples: int64(len(samples)),
		P50Ms:   pct(0.50),
		P95Ms:   pct(0.95),
		P99Ms:   pct(0.99),
		MaxMs:   ms(samples[len(samples)-1]),
		MeanMs:  sum / float64(len(samples)) / float64(time.Millisecond),
	}
}

func ms(ns int64) float64 { return float64(ns) / float64(time.Millisecond) }

// RetryDelays acumula (ts duplicado - ts original) en segundos. Sirve como
// evidencia de que lo detectado son reintentos reales (2-179 s en el
// generador) y no colisiones de event_id.
type RetryDelays struct {
	Count      int64   `json:"count"`
	MinSec     int64   `json:"min_sec"`
	MaxSec     int64   `json:"max_sec"`
	SumSec     int64   `json:"-"`
	MeanSec    float64 `json:"mean_sec"`
	OutOfRange int64   `json:"outside_2_to_180s"`
}

func (r *RetryDelays) Add(sec int64) {
	if r.Count == 0 || sec < r.MinSec {
		r.MinSec = sec
	}
	if r.Count == 0 || sec > r.MaxSec {
		r.MaxSec = sec
	}
	if sec < 2 || sec >= 180 {
		r.OutOfRange++
	}
	r.Count++
	r.SumSec += sec
}

func (r *RetryDelays) Merge(o RetryDelays) {
	if o.Count == 0 {
		return
	}
	if r.Count == 0 || o.MinSec < r.MinSec {
		r.MinSec = o.MinSec
	}
	if r.Count == 0 || o.MaxSec > r.MaxSec {
		r.MaxSec = o.MaxSec
	}
	r.Count += o.Count
	r.SumSec += o.SumSec
	r.OutOfRange += o.OutOfRange
}

func (r *RetryDelays) Finish() {
	if r.Count > 0 {
		r.MeanSec = float64(r.SumSec) / float64(r.Count)
	}
}

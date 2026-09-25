// Package agg mantiene los agregados de negocio por país y tipo de evento.
// Agg no tiene locks: cada shard del consumidor es dueño de su propio Agg y
// al final se suman con Merge.
package agg

import (
	"fmt"

	"hotsale/internal/event"
)

type Agg struct {
	Count [event.NumCountries][event.NumTypes]int64
	Cents [event.NumCountries][event.NumTypes]int64
}

func (a *Agg) Apply(e *event.Event) {
	a.Count[e.Country][e.Type]++
	a.Cents[e.Country][e.Type] += e.Cents
}

func (a *Agg) Merge(b *Agg) {
	for c := range a.Count {
		for t := range a.Count[c] {
			a.Count[c][t] += b.Count[c][t]
			a.Cents[c][t] += b.Cents[c][t]
		}
	}
}

// Line es la vista de negocio de un país (o del total).
type Line struct {
	Orders              int64  `json:"orders"`
	OrdersUSD           string `json:"orders_usd"`
	PaymentsConfirmed   int64  `json:"payments_confirmed"`
	PaymentsUSD         string `json:"payments_confirmed_usd"`
	InventoryReserved   int64  `json:"inventory_reserved"`
	UniqueEventsCounted int64  `json:"unique_events"`
}

type Report struct {
	ByCountry map[string]Line  `json:"by_country"`
	Total     Line             `json:"total"`
	ByType    map[string]int64 `json:"by_type"`
}

func (a *Agg) Report() Report {
	r := Report{ByCountry: map[string]Line{}, ByType: map[string]int64{}}
	var tot [event.NumTypes]int64
	var totCents [event.NumTypes]int64
	for c := range a.Count {
		r.ByCountry[event.CountryNames[c]] = line(a.Count[c], a.Cents[c])
		for t := range a.Count[c] {
			tot[t] += a.Count[c][t]
			totCents[t] += a.Cents[c][t]
		}
	}
	r.Total = line(tot, totCents)
	for t, n := range tot {
		r.ByType[event.TypeNames[t]] = n
	}
	return r
}

func line(count, cents [event.NumTypes]int64) Line {
	return Line{
		Orders:              count[event.OrderPlaced],
		OrdersUSD:           USD(cents[event.OrderPlaced]),
		PaymentsConfirmed:   count[event.PaymentConfirmed],
		PaymentsUSD:         USD(cents[event.PaymentConfirmed]),
		InventoryReserved:   count[event.InventoryReserved],
		UniqueEventsCounted: count[event.OrderPlaced] + count[event.PaymentConfirmed] + count[event.InventoryReserved],
	}
}

// USD formatea centavos como decimal exacto, sin pasar por float64.
func USD(cents int64) string {
	return fmt.Sprintf("%d.%02d", cents/100, cents%100)
}

// Diff compara dos reportes campo a campo y devuelve las diferencias.
func Diff(a, b Report) []string {
	var d []string
	cmp := func(scope string, x, y Line) {
		if x != y {
			d = append(d, fmt.Sprintf("%s: %+v != %+v", scope, x, y))
		}
	}
	for _, c := range event.CountryNames {
		cmp(c, a.ByCountry[c], b.ByCountry[c])
	}
	cmp("TOTAL", a.Total, b.Total)
	return d
}

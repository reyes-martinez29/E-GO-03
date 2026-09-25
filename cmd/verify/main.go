// Verify es el oráculo: lee el archivo directo (sin broker), deduplica con un
// map SIN ventana (recuerda todos los event_id) y calcula el agregado
// verdadero. Con -report compara contra el JSON que escribió el consumidor.
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"

	"hotsale/internal/agg"
	"hotsale/internal/event"
)

func main() {
	file := flag.String("file", "eventos_hotsale.jsonl", "archivo JSONL de eventos")
	reportPath := flag.String("report", "", "reporte JSON del consumidor a comparar (opcional)")
	flag.Parse()

	f, err := os.Open(*file)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	seen := make(map[event.ID]int64, 3_100_000) // event_id -> línea del original
	var a agg.Agg
	var lines, dups, maxDist int64
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
	for sc.Scan() {
		lines++
		e, err := event.Parse(sc.Bytes())
		if err != nil {
			log.Fatalf("línea %d: %v", lines, err)
		}
		if orig, ok := seen[e.ID]; ok {
			dups++
			maxDist = max(maxDist, lines-orig)
			continue
		}
		seen[e.ID] = lines
		a.Apply(&e)
	}
	if err := sc.Err(); err != nil {
		log.Fatal(err)
	}
	truth := a.Report()

	fmt.Printf("=== VERDAD (archivo, sin ventana) ===\n")
	fmt.Printf("líneas: %d  únicos: %d  duplicados: %d  distancia máx. original->reintento: %d línea(s)\n",
		lines, truth.Total.UniqueEventsCounted, dups, maxDist)
	fmt.Printf("por tipo: %v\n", truth.ByType)
	for _, c := range event.CountryNames {
		l := truth.ByCountry[c]
		fmt.Printf("%s órdenes=%d (%s USD) pagos=%d (%s USD) inventario=%d\n", c, l.Orders, l.OrdersUSD, l.PaymentsConfirmed, l.PaymentsUSD, l.InventoryReserved)
	}
	t := truth.Total
	fmt.Printf("TOTAL órdenes=%d (%s USD) pagos=%d (%s USD) inventario=%d\n", t.Orders, t.OrdersUSD, t.PaymentsConfirmed, t.PaymentsUSD, t.InventoryReserved)

	if *reportPath == "" {
		return
	}
	b, err := os.ReadFile(*reportPath)
	if err != nil {
		log.Fatal(err)
	}
	var rep struct {
		Counts struct {
			Duplicates int64 `json:"duplicates_discarded"`
		} `json:"counts"`
		Aggregate agg.Report `json:"aggregate"`
	}
	if err := json.Unmarshal(b, &rep); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\n=== COMPARACIÓN CON %s ===\n", *reportPath)
	ok := true
	if rep.Counts.Duplicates != dups {
		ok = false
		fmt.Printf("FALLA duplicados: consumidor=%d verdad=%d\n", rep.Counts.Duplicates, dups)
	} else {
		fmt.Printf("OK duplicados: %d == %d\n", rep.Counts.Duplicates, dups)
	}
	if diffs := agg.Diff(rep.Aggregate, truth); len(diffs) > 0 {
		ok = false
		for _, d := range diffs {
			fmt.Println("FALLA", d)
		}
	} else {
		fmt.Println("OK agregado idéntico al centavo en los 5 países y el total")
	}
	if !ok {
		os.Exit(1)
	}
}

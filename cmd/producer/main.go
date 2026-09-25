// Producer publica eventos_hotsale.jsonl al stream HOTSALE de JetStream, en el
// orden del archivo.
//
// Modelo: un solo lector secuencial + PublishMsgAsync con un límite de
// mensajes en vuelo. No hay worker pool ni json.Unmarshal: el productor
// publica los bytes crudos de cada línea, el orden del archivo se preserva y
// la RAM queda acotada a maxPending mensajes sin importar el tamaño del
// archivo. Un goroutine aparte confirma los PubAck en orden y reintenta los
// fallidos con el mismo Nats-Msg-Id (número de línea), para que el broker
// descarte el reenvío si el original sí había llegado.
package main

import (
	"bufio"
	"bytes"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime/pprof"
	"strconv"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"hotsale/internal/broker"
)

type pending struct {
	paf  jetstream.PubAckFuture
	msg  *nats.Msg
	line uint64
}

type ackStats struct {
	acked, brokerDup, retried, failed uint64
}

func main() {
	file := flag.String("file", "eventos_hotsale.jsonl", "archivo JSONL de eventos")
	url := flag.String("url", nats.DefaultURL, "URL de NATS")
	rate := flag.Int("rate", 0, "eventos/s a publicar (0 = sin límite, máximo throughput)")
	limit := flag.Uint64("limit", 0, "publicar solo las primeras N líneas (0 = todas)")
	// El Nats-Msg-Id debe ser único POR CORRIDA: con solo el número de línea,
	// una segunda corrida dentro de la ventana Duplicates del broker se
	// descartaba entera (ver decisions.md, hallazgo 5).
	runID := flag.String("run-id", strconv.FormatInt(time.Now().UnixNano(), 36), "prefijo del Nats-Msg-Id de esta corrida")
	// 1024: mismo throughput que 4096 con un tercio de la latencia; 16384 provoca
	// 429 del broker (ver decisions.md, barrido de max-pending).
	maxPending := flag.Int("max-pending", 1024, "máximo de publicaciones async sin PubAck (contrapresión)")
	cpuProfile := flag.String("cpuprofile", "", "escribir un CPU profile en este archivo")
	flag.Parse()

	if *cpuProfile != "" {
		pf, err := os.Create(*cpuProfile)
		if err != nil {
			log.Fatal(err)
		}
		if err := pprof.StartCPUProfile(pf); err != nil {
			log.Fatal(err)
		}
		defer pprof.StopCPUProfile()
	}

	nc, err := nats.Connect(*url, nats.Name("hotsale-producer"))
	if err != nil {
		log.Fatalf("conectar a NATS: %v", err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc, jetstream.WithPublishAsyncMaxPending(*maxPending))
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()
	if _, err := broker.EnsureStream(ctx, js); err != nil {
		log.Fatalf("crear stream: %v", err)
	}

	f, err := os.Open(*file)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	futures := make(chan pending, *maxPending)
	var st ackStats
	var wg sync.WaitGroup
	wg.Go(func() { confirmAcks(ctx, js, futures, &st) })

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
	var line uint64
	start := time.Now()
	lastLog := start
	for sc.Scan() {
		if *limit > 0 && line >= *limit {
			break
		}
		line++
		if *rate > 0 {
			pace(start, line, *rate)
		}
		msg := nats.NewMsg(broker.EventsSubject)
		msg.Data = bytes.Clone(sc.Bytes()) // Scanner reutiliza su buffer; el future retiene el *Msg
		msg.Header.Set(jetstream.MsgIDHeader, *runID+"-"+strconv.FormatUint(line, 10))
		msg.Header.Set(broker.HeaderPubTs, strconv.FormatInt(time.Now().UnixNano(), 10))
		futures <- pending{paf: publishAsync(js, msg), msg: msg, line: line}

		if now := time.Now(); now.Sub(lastLog) >= 5*time.Second {
			lastLog = now
			log.Printf("publicadas %d líneas (%.0f ev/s)", line, float64(line)/now.Sub(start).Seconds())
		}
	}
	if err := sc.Err(); err != nil {
		log.Fatalf("leer %s: %v", *file, err)
	}

	// Marcador de fin: el consumidor sabe cuántas líneas esperar y cuándo reportar.
	eof := nats.NewMsg(broker.EOFSubject)
	eof.Header.Set(broker.HeaderTotalLines, strconv.FormatUint(line, 10))
	eof.Header.Set(broker.HeaderPubTs, strconv.FormatInt(time.Now().UnixNano(), 10))
	futures <- pending{paf: publishAsync(js, eof), msg: eof}
	close(futures)
	wg.Wait()
	elapsed := time.Since(start)

	fmt.Printf("\n=== PRODUCTOR ===\n")
	fmt.Printf("líneas publicadas:      %d\n", line)
	fmt.Printf("PubAck confirmados:     %d (incluye EOF)\n", st.acked)
	fmt.Printf("reintentos del prod.:   %d\n", st.retried)
	fmt.Printf("descartados por broker: %d (Nats-Msg-Id repetido = reintento del productor)\n", st.brokerDup)
	fmt.Printf("fallidos definitivos:   %d\n", st.failed)
	fmt.Printf("tiempo:                 %v\n", elapsed.Round(time.Millisecond))
	fmt.Printf("throughput publicación: %.0f ev/s\n", float64(line)/elapsed.Seconds())
	if st.brokerDup > 0 {
		log.Printf("ATENCIÓN: el broker descartó %d mensajes por Nats-Msg-Id repetido; si no hubo reintentos del productor, ¿se reutilizó -run-id?", st.brokerDup)
	}
	if st.failed > 0 {
		pprof.StopCPUProfile()
		os.Exit(1)
	}
}

// publishAsync bloquea (con contrapresión) mientras haya maxPending mensajes
// sin confirmar, en lugar de fallar con ErrTooManyStalledMsgs.
func publishAsync(js jetstream.JetStream, msg *nats.Msg) jetstream.PubAckFuture {
	for {
		paf, err := js.PublishMsgAsync(msg, jetstream.WithStallWait(time.Second))
		if err == nil {
			return paf
		}
		if err == jetstream.ErrTooManyStalledMsgs {
			continue
		}
		log.Fatalf("publicar: %v", err)
	}
}

// confirmAcks espera los PubAck en orden de publicación. Un fallo se
// reintenta de forma síncrona con el mismo Nats-Msg-Id.
func confirmAcks(ctx context.Context, js jetstream.JetStream, futures <-chan pending, st *ackStats) {
	for p := range futures {
		select {
		case ack := <-p.paf.Ok():
			st.acked++
			if ack.Duplicate {
				st.brokerDup++
			}
		case err := <-p.paf.Err():
			if st.retried < 5 {
				log.Printf("línea %d: PubAck falló (%v); reintentando (se omiten los siguientes avisos)", p.line, err)
			}
			// Mensaje nuevo: el async dejó su propio inbox en Reply.
			retry := &nats.Msg{Subject: p.msg.Subject, Data: p.msg.Data, Header: p.msg.Header}
			ok := false
			for attempt := 1; attempt <= 5 && !ok; attempt++ {
				st.retried++
				rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
				ack, err := js.PublishMsg(rctx, retry)
				cancel()
				if err == nil {
					ok = true
					st.acked++
					if ack.Duplicate {
						st.brokerDup++
					}
				} else {
					time.Sleep(time.Duration(attempt) * 200 * time.Millisecond)
				}
			}
			if !ok {
				st.failed++
				log.Printf("línea %d: se perdió tras 5 reintentos", p.line)
			}
		}
	}
}

// pace duerme lo necesario para que la línea n salga en start + n/rate.
// Se programa contra el reloj absoluto (no un sleep fijo por mensaje) para
// que los retrasos no se acumulen.
func pace(start time.Time, n uint64, rate int) {
	target := start.Add(time.Duration(float64(n) / float64(rate) * float64(time.Second)))
	if d := time.Until(target); d > time.Millisecond {
		time.Sleep(d)
	}
}

// Consumer consume el stream HOTSALE, deduplica por event_id y mantiene los
// agregados por país y tipo de evento.
//
// Pipeline (sin locks sobre el estado de negocio):
//
//	fetcher (1) ──▶ shards (M: json decode + dedup + agregado) ──▶ merge final
//	      shard = hash(event_id) % M, con event_id extraído de los bytes crudos
//
// Cada shard es el único dueño de su ventana de dedup y de su Agg, y un mismo
// event_id siempre cae en el mismo shard: el "¿ya lo vi? → márcalo → súmalo"
// es atómico por construcción, sin sync.Mutex ni RWMutex. El fetcher es
// secuencial y cada shard es FIFO, así que el orden por event_id se preserva
// (un pool de decoders intermedio lo rompía, ver decisions.md). El json decode
// sigue siendo paralelo: ocurre dentro de los M shards. El ack se envía solo
// después de aplicar el evento, por lotes y en orden (ver commit.go).
//
// El agregado vive solo en memoria (decisión explícita, ver decisions.md): si
// el proceso se cae, al reiniciar se re-consume el stream desde el inicio y
// ventana y agregado se reconstruyen juntos.
package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"hotsale/internal/agg"
	"hotsale/internal/broker"
	"hotsale/internal/dedup"
	"hotsale/internal/event"
	"hotsale/internal/metrics"
)

type config struct {
	URL          string        `json:"-"`
	Shards       int           `json:"shards"`
	Window       time.Duration `json:"dedup_window"`
	Generations  int           `json:"dedup_generations"`
	MinRetention time.Duration `json:"dedup_min_retention"`
	MaxPerGen    int           `json:"dedup_max_per_generation"`
	PullBatch    int           `json:"pull_max_messages"`
	MaxAckPend   int           `json:"max_ack_pending"`
	Purge        bool          `json:"purge_stream_on_start"`
	AckBatch     int           `json:"ack_batch"`
	AckEvery     time.Duration `json:"ack_max_interval"`
	Label        string        `json:"label"`
}

type shard struct {
	in      chan work
	win     *dedup.Window
	agg     agg.Agg
	invalid *atomic.Int64

	applied        int64 // eventos únicos aplicados al agregado
	dups           int64 // duplicados del origen (reintentos del gateway) descartados
	redelivered    int64 // re-entregas del broker (NumDelivered > 1)
	redeliveredDup int64 // re-entregas que la ventana reconoció y descartó
	delays         metrics.RetryDelays
	lat            []int64
	minPubTs       int64
	lastApply      int64

	processed atomic.Int64 // para el log de progreso
	winLen    atomic.Int64
}

func main() {
	var cfg config
	flag.StringVar(&cfg.URL, "url", nats.DefaultURL, "URL de NATS")
	flag.IntVar(&cfg.Shards, "shards", 8, "shards de agregación (cada uno con su ventana de dedup)")
	flag.DurationVar(&cfg.Window, "dedup-window", 5*time.Minute, "retención nominal de la ventana de dedup")
	flag.IntVar(&cfg.Generations, "dedup-gens", 10, "generaciones del anillo de dedup")
	flag.DurationVar(&cfg.MinRetention, "dedup-min", 3*time.Minute, "retención mínima garantizada (retraso máximo de reintento)")
	flag.IntVar(&cfg.MaxPerGen, "dedup-max-per-gen", 0, "tope de llaves por generación y shard (0 = sin tope)")
	flag.IntVar(&cfg.PullBatch, "pull", 4096, "mensajes a pedir por pull al broker")
	flag.IntVar(&cfg.MaxAckPend, "max-ack-pending", 65536, "mensajes entregados sin ack permitidos por el broker")
	flag.BoolVar(&cfg.Purge, "purge", true, "vaciar el stream al arrancar (corrida nueva); false = re-consumir lo que haya (reinicio)")
	flag.IntVar(&cfg.AckBatch, "ack-batch", 1000, "mensajes por lote de ack (AckAll)")
	flag.DurationVar(&cfg.AckEvery, "ack-every", 10*time.Millisecond, "intervalo máximo para cerrar un lote de ack incompleto")
	flag.StringVar(&cfg.Label, "label", "", "etiqueta de la corrida para el reporte")
	pprofAddr := flag.String("pprof", "localhost:6060", "dirección de net/http/pprof (vacío = desactivado)")
	outDir := flag.String("out", "results", "carpeta para el reporte JSON y el heap profile")
	flag.Parse()

	if *pprofAddr != "" {
		go func() { log.Println(http.ListenAndServe(*pprofAddr, nil)) }()
	}

	nc, err := nats.Connect(cfg.URL, nats.Name("hotsale-consumer"))
	if err != nil {
		log.Fatalf("conectar a NATS: %v", err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()
	stream, err := broker.EnsureStream(ctx, js)
	if err != nil {
		log.Fatalf("crear stream: %v", err)
	}
	if cfg.Purge {
		if err := stream.Purge(ctx); err != nil {
			log.Fatalf("purgar stream: %v", err)
		}
	}
	// Consumer nuevo en cada arranque con DeliverAll: sin agregado persistido,
	// reiniciar significa reconstruir desde el primer mensaje retenido.
	if err := js.DeleteConsumer(ctx, broker.StreamName, broker.ConsumerName); err != nil && !errors.Is(err, jetstream.ErrConsumerNotFound) {
		log.Fatalf("borrar consumer: %v", err)
	}
	cons, err := js.CreateOrUpdateConsumer(ctx, broker.StreamName, jetstream.ConsumerConfig{
		Durable:       broker.ConsumerName,
		AckPolicy:     jetstream.AckAllPolicy, // ack de S confirma todo <= S; ver commit.go
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckWait:       60 * time.Second,
		MaxAckPending: cfg.MaxAckPend,
	})
	if err != nil {
		log.Fatalf("crear consumer: %v", err)
	}
	it, err := cons.Messages(jetstream.PullMaxMessages(cfg.PullBatch))
	if err != nil {
		log.Fatal(err)
	}

	var invalid atomic.Int64
	shards := make([]*shard, cfg.Shards)
	for i := range shards {
		w, err := dedup.New(dedup.Config{Window: cfg.Window, Generations: cfg.Generations, MinRetention: cfg.MinRetention, MaxPerGen: cfg.MaxPerGen})
		if err != nil {
			log.Fatal(err)
		}
		shards[i] = &shard{in: make(chan work, 1024), win: w, invalid: &invalid, minPubTs: 1<<63 - 1}
	}
	var shardWG sync.WaitGroup
	for _, s := range shards {
		shardWG.Go(s.run)
	}

	var received atomic.Int64
	var peakHeap atomic.Uint64
	stopMon := monitor(shards, &received, &invalid, &peakHeap)

	log.Printf("consumidor listo (shards=%d, ventana=%v/%d gen, retención garantizada=%v); esperando eventos...",
		cfg.Shards, cfg.Window, cfg.Generations, shards[0].win.GuaranteedRetention())

	// it.Next() bloquea; un goroutine lector permite cerrar lotes por tiempo
	// cuando el tráfico es bajo.
	msgs := make(chan jetstream.Msg, cfg.PullBatch)
	go func() {
		for {
			msg, err := it.Next()
			if err != nil {
				if !errors.Is(err, jetstream.ErrMsgIteratorClosed) {
					log.Fatalf("leer del stream: %v", err)
				}
				return
			}
			msgs <- msg
		}
	}()
	batches := make(chan *batch, 1024)
	var acks atomic.Int64
	var commitWG sync.WaitGroup
	commitWG.Go(func() { committer(batches, &acks) })

	var firstRecv time.Time
	var totalLines int64 = -1
	var eofMsg jetstream.Msg
	cur := newBatch()
	sealTick := time.NewTicker(cfg.AckEvery)
	seal := func() {
		if cur.size == 0 {
			return
		}
		cur.seal()
		batches <- cur
		cur = newBatch()
	}
	for eofMsg == nil {
		var msg jetstream.Msg
		select {
		case msg = <-msgs:
		case <-sealTick.C:
			seal()
			continue
		}
		if firstRecv.IsZero() {
			firstRecv = time.Now()
		}
		if msg.Subject() == broker.EOFSubject {
			totalLines, _ = strconv.ParseInt(msg.Headers().Get(broker.HeaderTotalLines), 10, 64)
			eofMsg = msg
			break
		}
		received.Add(1)
		id, err := event.ExtractID(msg.Data())
		if err != nil {
			reject(msg, err, &invalid)
			continue
		}
		cur.add(msg)
		// El UUID v4 es aleatorio: sus primeros 8 bytes ya son un buen hash.
		shards[binary.LittleEndian.Uint64(id[:8])%uint64(len(shards))].in <- work{msg: msg, b: cur}
		if cur.size >= cfg.AckBatch {
			seal()
		}
	}
	sealTick.Stop()
	seal()
	close(batches)
	commitWG.Wait()
	_ = eofMsg.Ack() // con AckAll confirma también todo lo anterior
	acks.Add(1)
	it.Stop()
	for _, s := range shards {
		close(s.in)
	}
	shardWG.Wait()
	stopMon()
	if err := nc.Flush(); err != nil { // asegura que los últimos acks salgan
		log.Printf("flush de acks: %v", err)
	}

	// Heap profile con las ventanas todavía vivas: muestra cuánto ocupa la dedup.
	_ = os.MkdirAll(*outDir, 0o755)
	stamp := time.Now().Format("20060102-150405")
	heapPath := filepath.Join(*outDir, "heap-"+stamp+".pb.gz")
	writeHeapProfile(heapPath)

	rep := buildReport(cfg, shards, firstRecv, received.Load(), invalid.Load(), totalLines, peakHeap.Load())
	rep.HeapProfile = heapPath
	rep.Counts.Acks = acks.Load()
	printReport(rep)
	path := filepath.Join(*outDir, "consumer-"+stamp+".json")
	b, _ := json.MarshalIndent(rep, "", "  ")
	if err := os.WriteFile(path, b, 0o644); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\nreporte: %s\nheap profile: %s\n", path, heapPath)
	runtime.KeepAlive(shards)
}

func reject(msg jetstream.Msg, err error, invalid *atomic.Int64) {
	if invalid.Add(1) <= 5 {
		log.Printf("evento inválido descartado: %v: %.120s", err, msg.Data())
	}
	_ = msg.Term() // no reintentar: nunca va a parsear
}

func (s *shard) run() {
	for w := range s.in {
		s.handle(w.msg)
		w.b.finish()
		if n := s.processed.Add(1); n&1023 == 0 {
			s.winLen.Store(int64(s.win.Stats().Len))
		}
	}
	s.winLen.Store(int64(s.win.Stats().Len))
}

// handle aplica un mensaje: parseo, dedup y agregado. Sin ack: lo hace el
// committer cuando todo el lote está aplicado.
func (s *shard) handle(msg jetstream.Msg) {
	ev, err := event.Parse(msg.Data())
	if err != nil {
		reject(msg, err, s.invalid)
		return
	}
	pubTs, _ := strconv.ParseInt(msg.Headers().Get(broker.HeaderPubTs), 10, 64)
	redelivered := false
	if md, err := msg.Metadata(); err == nil && md.NumDelivered > 1 {
		redelivered = true
	}

	dup, origTS := s.win.SeenOrAdd(ev.ID, ev.TS)
	switch {
	case redelivered:
		s.redelivered++
		if dup {
			s.redeliveredDup++
		} else { // la entrega original nunca llegó a aplicarse
			s.agg.Apply(&ev)
			s.applied++
		}
	case dup:
		s.dups++
		s.delays.Add(ev.TS - origTS)
	default:
		s.agg.Apply(&ev)
		s.applied++
	}
	now := time.Now().UnixNano()
	if !redelivered && pubTs > 0 {
		s.lat = append(s.lat, now-pubTs)
		s.minPubTs = min(s.minPubTs, pubTs)
	}
	s.lastApply = now
}

// monitor imprime progreso cada 5 s y muestrea el heap cada segundo.
func monitor(shards []*shard, received, invalid *atomic.Int64, peakHeap *atomic.Uint64) (stop func()) {
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		var ms runtime.MemStats
		var lastProcessed int64
		for i := 1; ; i++ {
			select {
			case <-done:
				return
			case <-tick.C:
			}
			runtime.ReadMemStats(&ms)
			if ms.HeapInuse > peakHeap.Load() {
				peakHeap.Store(ms.HeapInuse)
			}
			if i%5 != 0 {
				continue
			}
			var processed, winLen int64
			for _, s := range shards {
				processed += s.processed.Load()
				winLen += s.winLen.Load()
			}
			if processed == lastProcessed && processed == 0 {
				continue
			}
			log.Printf("recibidos=%d procesados=%d (%.0f ev/s) ventana=%d llaves heap=%d MiB inválidos=%d",
				received.Load(), processed, float64(processed-lastProcessed)/5, winLen, ms.HeapInuse>>20, invalid.Load())
			lastProcessed = processed
		}
	})
	return func() { close(done); wg.Wait() }
}

func writeHeapProfile(path string) {
	f, err := os.Create(path)
	if err != nil {
		log.Printf("heap profile: %v", err)
		return
	}
	defer f.Close()
	runtime.GC()
	if err := pprof.WriteHeapProfile(f); err != nil {
		log.Printf("heap profile: %v", err)
	}
}

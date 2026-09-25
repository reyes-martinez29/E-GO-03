package dedup

import (
	"encoding/binary"
	"testing"
	"time"
)

type fakeClock struct{ t time.Time }

func (c *fakeClock) Now() time.Time          { return c.t }
func (c *fakeClock) Advance(d time.Duration) { c.t = c.t.Add(d) }

func key(i uint64) Key {
	var k Key
	binary.LittleEndian.PutUint64(k[:], i)
	return k
}

func newTestWindow(t *testing.T, clk *fakeClock, maxPerGen int) *Window {
	t.Helper()
	w, err := New(Config{Window: 5 * time.Minute, Generations: 10, MinRetention: 3 * time.Minute, MaxPerGen: maxPerGen, Now: clk.Now})
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func TestDuplicateDetected(t *testing.T) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	w := newTestWindow(t, clk, 0)
	if dup, _ := w.SeenOrAdd(key(1), 100); dup {
		t.Fatal("primer evento marcado como duplicado")
	}
	dup, orig := w.SeenOrAdd(key(1), 150)
	if !dup || orig != 100 {
		t.Fatalf("dup=%v orig=%d; quería true, 100", dup, orig)
	}
	if dup, _ := w.SeenOrAdd(key(2), 100); dup {
		t.Fatal("evento distinto marcado como duplicado")
	}
}

// Los reintentos del dataset llegan hasta 179 s después (rng.integers(2, 180)).
// Un reintento en el peor caso debe detectarse sin importar en qué punto de
// la generación actual entró el original.
func TestRetryAtWorstCaseDelayIsDetected(t *testing.T) {
	for offset := time.Duration(0); offset < 30*time.Second; offset += 5 * time.Second {
		clk := &fakeClock{t: time.Unix(0, 0)}
		w := newTestWindow(t, clk, 0)
		clk.Advance(offset)
		w.SeenOrAdd(key(1), 0)
		clk.Advance(179 * time.Second)
		if dup, _ := w.SeenOrAdd(key(1), 179); !dup {
			t.Fatalf("offset %v: reintento a 179 s no detectado", offset)
		}
	}
}

func TestGuaranteedRetentionBoundary(t *testing.T) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	w := newTestWindow(t, clk, 0)
	if got := w.GuaranteedRetention(); got != 270*time.Second {
		t.Fatalf("retención garantizada = %v; quería 4m30s", got)
	}
	// Insertado al final de la generación: vive exactamente la garantía.
	clk.Advance(30*time.Second - time.Nanosecond)
	w.SeenOrAdd(key(1), 0)
	clk.Advance(270*time.Second - time.Nanosecond)
	if dup, _ := w.SeenOrAdd(key(1), 0); !dup {
		t.Fatal("expiró antes de la retención garantizada")
	}
	// Una llave vieja (> Window) debe haber expirado.
	clk.Advance(5 * time.Minute)
	if dup, _ := w.SeenOrAdd(key(1), 0); dup {
		t.Fatal("llave no expiró después de la ventana completa")
	}
}

// La ventana no debe crecer sin límite: con tráfico constante de llaves
// nuevas, el tamaño se estabiliza en ~ tasa × Window.
func TestSizeIsBoundedUnderSteadyTraffic(t *testing.T) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	w := newTestWindow(t, clk, 0)
	const perSecond = 100
	maxLen := 0
	var i uint64
	for sec := 0; sec < 3600; sec++ { // 1 hora simulada = 12 ventanas
		for j := 0; j < perSecond; j++ {
			w.SeenOrAdd(key(i), int64(sec))
			i++
		}
		clk.Advance(time.Second)
		maxLen = max(maxLen, w.Stats().Len)
	}
	if limit := perSecond * 300; maxLen > limit {
		t.Fatalf("tamaño máximo %d supera tasa×ventana = %d", maxLen, limit)
	}
	if st := w.Stats(); st.Premature != 0 {
		t.Fatalf("expulsiones prematuras sin tope de memoria: %d", st.Premature)
	}
}

func TestLongIdleExpiresEverything(t *testing.T) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	w := newTestWindow(t, clk, 0)
	for i := uint64(0); i < 1000; i++ {
		w.SeenOrAdd(key(i), 0)
	}
	clk.Advance(24 * time.Hour)
	w.SeenOrAdd(key(5000), 0)
	if st := w.Stats(); st.Len != 1 || st.Premature != 0 {
		t.Fatalf("stats tras inactividad = %+v; quería len=1 premature=0", st)
	}
}

func TestMaxPerGenCountsPrematureEvictions(t *testing.T) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	w := newTestWindow(t, clk, 10)
	for i := uint64(0); i < 200; i++ { // 200 llaves en el mismo instante: fuerza rotaciones
		w.SeenOrAdd(key(i), 0)
	}
	st := w.Stats()
	if st.Premature == 0 {
		t.Fatal("el tope de memoria expulsó llaves jóvenes y no se contabilizó")
	}
	if st.Len > 100 {
		t.Fatalf("len=%d supera el tope 10 llaves × 10 generaciones", st.Len)
	}
}

func TestInvalidConfig(t *testing.T) {
	if _, err := New(Config{Window: 3 * time.Minute, Generations: 10, MinRetention: 3 * time.Minute}); err == nil {
		t.Fatal("una ventana de 3m con 10 generaciones solo garantiza 2m42s; debía rechazarse")
	}
}

func BenchmarkSeenOrAdd(b *testing.B) {
	w, _ := New(Config{Window: 5 * time.Minute, Generations: 10, MinRetention: 3 * time.Minute})
	b.ReportAllocs()
	var i uint64
	for b.Loop() {
		w.SeenOrAdd(key(i), 0)
		i++
	}
}

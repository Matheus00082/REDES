package peer

// Testes de integração: sobem o tracker e vários nós de verdade em 127.0.0.1
// e conferem que todos terminam com o arquivo idêntico ao original.

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"minitorrent/internal/manifest"
	"minitorrent/internal/tracker"
)

type testSwarm struct {
	t       *testing.T
	dir     string
	file    string
	data    []byte
	m       *manifest.Manifest
	tracker string
	ctx     context.Context
	errs    []chan error
}

func newTestSwarm(t *testing.T, size int) *testSwarm {
	t.Helper()
	dir := t.TempDir()
	data := make([]byte, size)
	rand.Read(data)
	file := filepath.Join(dir, "original.bin")
	if err := os.WriteFile(file, data, 0o644); err != nil {
		t.Fatal(err)
	}
	m, err := manifest.Create(file, 64<<10)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	sw := &testSwarm{t: t, dir: dir, file: file, data: data, m: m, ctx: ctx}
	sw.tracker = startTracker(t, ctx)
	t.Cleanup(func() {
		cancel()
		for _, errc := range sw.errs { // espera cada nó encerrar de forma limpa
			select {
			case err := <-errc:
				if err != nil {
					t.Errorf("Run devolveu erro: %v", err)
				}
			case <-time.After(10 * time.Second):
				t.Error("um nó não encerrou em 10s")
			}
		}
	})
	return sw
}

// startTracker sobe o tracker do pacote tracker numa porta livre.
func startTracker(t *testing.T, ctx context.Context) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	go tracker.NewServer(100*time.Millisecond, time.Second).Run(ctx, addr)
	for i := 0; i < 100; i++ {
		if c, err := net.Dial("tcp", addr); err == nil {
			c.Close()
			return "http://" + addr
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("tracker não subiu")
	return ""
}

// start cria um nó e o põe para rodar em segundo plano.
func (sw *testSwarm) start(cfg Config) *Node {
	sw.t.Helper()
	cfg.TrackerURL = sw.tracker
	cfg.ListenAddr = "127.0.0.1:0"
	if cfg.Seed {
		cfg.DataPath = sw.file
	} else {
		cfg.DataPath = filepath.Join(sw.dir, cfg.ID+".bin")
	}
	n, err := New(cfg, sw.m)
	if err != nil {
		sw.t.Fatal(err)
	}
	errc := make(chan error, 1)
	go func() { errc <- n.Run(sw.ctx) }()
	sw.errs = append(sw.errs, errc)
	return n
}

// waitDone espera os nós completarem e confere o conteúdo baixado.
func (sw *testSwarm) waitDone(nodes ...*Node) {
	sw.t.Helper()
	timeout := time.After(30 * time.Second)
	for _, n := range nodes {
		select {
		case <-n.Done():
		case <-timeout:
			sw.t.Fatalf("%s não terminou o download a tempo", n.cfg.ID)
		}
		if got, _ := os.ReadFile(n.cfg.DataPath); !bytes.Equal(got, sw.data) {
			sw.t.Fatalf("%s: arquivo baixado difere do original", n.cfg.ID)
		}
	}
}

func bytesFrom(n *Node) map[string]int64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := map[string]int64{}
	for id, b := range n.bytesFrom {
		out[id] = b
	}
	return out
}

func TestP2P(t *testing.T) {
	for _, picker := range []string{PickRarest, PickSequential} {
		t.Run(picker, func(t *testing.T) {
			sw := newTestSwarm(t, 4<<20)
			sw.start(Config{ID: "seed", Seed: true})
			var peers []*Node
			for i := 1; i <= 4; i++ {
				peers = append(peers, sw.start(Config{ID: fmt.Sprintf("p%d", i), Picker: picker, Linger: true}))
			}
			sw.waitDone(peers...)
		})
	}
}

func TestClientServer(t *testing.T) {
	sw := newTestSwarm(t, 2<<20)
	sw.start(Config{ID: "seed", Seed: true})
	var clients []*Node
	for i := 1; i <= 3; i++ {
		// Linger mantém os clientes vivos: mesmo assim, ninguém pode baixar de um cliente.
		clients = append(clients, sw.start(Config{ID: fmt.Sprintf("c%d", i), Mode: ModeCS, Linger: true}))
	}
	sw.waitDone(clients...)
	for _, c := range clients {
		for id := range bytesFrom(c) {
			if id != "seed" {
				t.Fatalf("%s recebeu dados de %s; no modo cliente-servidor só a semente envia", c.cfg.ID, id)
			}
		}
		if up := c.uploaded.Load(); up != 0 {
			t.Fatalf("%s enviou %d bytes; cliente do modo cliente-servidor não deve enviar nada", c.cfg.ID, up)
		}
	}
}

func TestMaliciousPeerIsBanned(t *testing.T) {
	sw := newTestSwarm(t, 2<<20)
	// No começo só existe o par malicioso, que corrompe tudo o que envia.
	sw.start(Config{ID: "evil", Seed: true, Corrupt: 1})
	p1 := sw.start(Config{ID: "p1", Linger: true})
	p2 := sw.start(Config{ID: "p2", Linger: true})

	deadline := time.Now().Add(10 * time.Second)
	for _, p := range []*Node{p1, p2} {
		for {
			p.mu.Lock()
			banned := p.banned["evil"]
			p.mu.Unlock()
			if banned {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s não baniu o par malicioso", p.cfg.ID)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	for _, p := range []*Node{p1, p2} {
		// todo bloco do malicioso é corrompido: nada dele pode ter ido para o disco
		if have, _ := p.store.HaveCount(); have != 0 {
			t.Fatalf("%s gravou %d blocos vindos do par malicioso", p.cfg.ID, have)
		}
	}

	// Agora entra uma semente honesta: os dois devem terminar com o arquivo certo.
	sw.start(Config{ID: "seed", Seed: true})
	sw.waitDone(p1, p2)
}

func TestSeedLeavesAfterUpload(t *testing.T) {
	sw := newTestSwarm(t, 1<<20)
	sw.start(Config{ID: "seed", Seed: true, LeaveAfter: int64(len(sw.data))})
	seedErr := sw.errs[0]
	p1 := sw.start(Config{ID: "p1"})
	sw.waitDone(p1)
	select {
	case err := <-seedErr: // Run da semente retornou: ela saiu sozinha depois de enviar 1× o arquivo
		seedErr <- err // devolve para a limpeza do teste conferir
	case <-time.After(5 * time.Second):
		t.Fatal("a semente não saiu depois de enviar o arquivo uma vez")
	}
}

func TestPickRarest(t *testing.T) {
	avail := []int{3, 1, 2, 1, 5}
	notOne := func(i int) bool { return i != 1 } // o bloco 1 já foi pedido
	for k := 0; k < 50; k++ {
		if got := pickRarest(avail, notOne); got != 3 {
			t.Fatalf("esperava o bloco 3 (o mais raro disponível), veio %d", got)
		}
	}
	seen := map[int]bool{}
	for k := 0; k < 300; k++ {
		seen[pickRarest([]int{1, 1, 1}, func(int) bool { return true })] = true
	}
	if len(seen) != 3 {
		t.Fatalf("o desempate deveria sortear entre os 3 blocos, viu só %v", seen)
	}
	if got := pickRarest(avail, func(int) bool { return false }); got != -1 {
		t.Fatalf("sem candidatos deveria dar -1, veio %d", got)
	}
}

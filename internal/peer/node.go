// Package peer implementa um nó da rede P2P, que é ao mesmo tempo cliente
// (baixa blocos dos vizinhos) e servidor (envia blocos a quem pedir).
//
//	node.go      ciclo de vida do nó, tracker, vizinhos e os modos p2p e cliente-servidor
package peer

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"minitorrent/internal/manifest"
	"minitorrent/internal/ratelimit"
	"minitorrent/internal/tracker"
	"minitorrent/internal/wire"
)

// Modos de operação.
const (
	ModeP2P = "p2p" // todos baixam e repassam blocos uns para os outros
	ModeCS  = "cs"  // cliente-servidor: clientes só baixam da semente e não repassam nada
)

const (
	pipeline           = 8                // pedidos em aberto por vizinho
	maxStrikes         = 3                // blocos corrompidos tolerados antes de banir
	maxTimeouts        = 3                // timeouts consecutivos de blocos antes de desconectar
	keepAliveEvery     = 5 * time.Second  // heartbeat entre vizinhos
	deadAfter          = 30 * time.Second // nada recebido nesse tempo = vizinho morto
	requestTimeout     = 15 * time.Second // prazo para um pedido de bloco ser atendido
	inflightCheckEvery = 2 * time.Second  // frequência de verificação de pedidos expirados
	dialTimeout        = 3 * time.Second
	handshakeTimeout   = 5 * time.Second
	closeGrace         = 2 * time.Second // tempo para o vizinho receber o que já enviamos, ao sair
)

// Config reúne os parâmetros de um nó (vêm das flags da linha de comando).
type Config struct {
	ID            string  // nome do peer; precisa ser único no enxame
	DataPath      string  // semente: o arquivo original; download: o destino
	Seed          bool    // true = começa com o arquivo completo (no modo cs, é o servidor)
	Mode          string  // ModeP2P ou ModeCS
	ListenAddr    string  // ip:porta para aceitar conexões
	AdvertiseAddr string  // ip:porta anunciado ao tracker (se vazio, usa o endereço real de ListenAddr)
	TrackerURL    string  // ex.: http://10.10.0.254:8000
	Picker        string  // PickRarest ou PickSequential
	Linger        bool    // continuar semeando depois de completar o download
	LeaveAfter    int64   // sair depois de enviar este total de bytes (0 = nunca)
	Corrupt       float64 // probabilidade [0, 1] de corromper cada bloco enviado (simula par malicioso)
	UpLimit       int64   // limite global de upload em bytes/s (0 = ilimitado)
	DownLimit     int64   // limite global de download em bytes/s (0 = ilimitado)
	ResultPath    string  // JSON com estatísticas, gravado ao completar o download
}

// Node é um peer em execução.
type Node struct {
	cfg           Config
	m             *manifest.Manifest
	infoHash      string // SHA-256 do manifesto em hex: identifica o arquivo no tracker e no handshake
	store         *Storage
	picker        *Picker
	upLimiter     *ratelimit.Limiter
	downLimiter   *ratelimit.Limiter
	listenAddr    string // endereço real de escuta (resolve a porta se veio ":0")
	advertiseAddr string // endereço anunciado ao tracker
	log           *log.Logger

	ctx  context.Context
	stop context.CancelFunc
	wg   sync.WaitGroup // goroutines que precisam terminar antes de fechar o arquivo

	mu        sync.Mutex          // protege os campos abaixo
	avail     []int               // em quantos vizinhos conectados cada bloco está disponível
	inflight  map[int]inflightReq // bloco -> pedido em andamento
	conns     map[string]*Conn    // vizinhos conectados, por id
	dialing   map[string]bool     // conexões que estamos abrindo
	banned    map[string]bool     // vizinhos banidos por mandar blocos corrompidos
	bytesFrom map[string]int64    // bytes úteis recebidos de cada vizinho

	uploaded   atomic.Int64
	start      time.Time
	done       chan struct{} // fechado quando o arquivo está completo
	finishOnce sync.Once
	leaveOnce  sync.Once
}

// New prepara um nó: abre o arquivo (a semente confere se ele bate com o
// manifesto; quem baixa aproveita blocos de um download interrompido).
func New(cfg Config, m *manifest.Manifest) (*Node, error) {
	if m == nil {
		return nil, errors.New("manifesto não pode ser nulo")
	}
	if cfg.ID == "" {
		cfg.ID = fmt.Sprintf("peer-%06d", rand.Intn(1_000_000))
	}
	if cfg.Mode == "" {
		cfg.Mode = ModeP2P
	}
	if cfg.Picker == "" {
		cfg.Picker = PickRarest
	}
	switch {
	case cfg.Mode != ModeP2P && cfg.Mode != ModeCS:
		return nil, fmt.Errorf("modo desconhecido %q (use %s ou %s)", cfg.Mode, ModeP2P, ModeCS)
	case cfg.ListenAddr == "":
		return nil, errors.New("é preciso um endereço para escutar: o tracker exige um addr no anúncio")
	case cfg.TrackerURL == "":
		return nil, errors.New("é preciso a URL do tracker")
	case math.IsNaN(cfg.Corrupt) || math.IsInf(cfg.Corrupt, 0) || cfg.Corrupt < 0 || cfg.Corrupt > 1:
		return nil, fmt.Errorf("corrupt deve estar entre 0 e 1 (recebido %v)", cfg.Corrupt)
	case cfg.LeaveAfter < 0:
		return nil, fmt.Errorf("leave-after não pode ser negativo (recebido %d)", cfg.LeaveAfter)
	case cfg.UpLimit < 0:
		return nil, fmt.Errorf("up-limit não pode ser negativo (recebido %d)", cfg.UpLimit)
	case cfg.DownLimit < 0:
		return nil, fmt.Errorf("down-limit não pode ser negativo (recebido %d)", cfg.DownLimit)
	}

	picker, err := NewPickerFromName(cfg.Picker)
	if err != nil {
		return nil, err
	}

	store, err := OpenStorage(cfg.DataPath, m, cfg.Seed)
	if err != nil {
		return nil, err
	}
	if cfg.Seed && !store.IsComplete() {
		have, total := store.HaveCount()
		store.Close()
		return nil, fmt.Errorf("%s não confere com o manifesto: só %d de %d blocos válidos", cfg.DataPath, have, total)
	}

	var upLimiter, downLimiter *ratelimit.Limiter
	if cfg.UpLimit > 0 {
		upLimiter = ratelimit.New(cfg.UpLimit)
	}
	if cfg.DownLimit > 0 {
		downLimiter = ratelimit.New(cfg.DownLimit)
	}

	ih := m.InfoHash()
	n := &Node{
		cfg:         cfg,
		m:           m,
		infoHash:    hex.EncodeToString(ih[:]),
		store:       store,
		picker:      picker,
		upLimiter:   upLimiter,
		downLimiter: downLimiter,
		log:         log.New(os.Stderr, "["+cfg.ID+"] ", log.Ltime|log.Lmicroseconds),
		avail:       make([]int, m.NumPieces()),
		inflight:    map[int]inflightReq{},
		conns:       map[string]*Conn{},
		dialing:     map[string]bool{},
		banned:      map[string]bool{},
		bytesFrom:   map[string]int64{},
		done:        make(chan struct{}),
	}
	// Para sementes, o arquivo já nasce completo e não gera arquivo de resultado de download.
	// Para peers comuns (get) com arquivo já completo em disco, finish() é chamado em Run()
	// para registrar o log e gravar o JSON de resultado (-result).
	if cfg.Seed {
		close(n.done)
	}
	return n, nil
}

// Done é fechado quando o nó tem o arquivo completo.
func (n *Node) Done() <-chan struct{} { return n.done }

// Run executa o nó até ctx ser cancelado (Ctrl+C), ou até o download
// terminar se o nó não for semente nem tiver Linger.
func (n *Node) Run(ctx context.Context) error {
	n.ctx, n.stop = context.WithCancel(ctx)
	defer n.stop()

	ln, err := net.Listen("tcp", n.cfg.ListenAddr)
	if err != nil {
		n.store.Close()
		return fmt.Errorf("escutar em %s: %w", n.cfg.ListenAddr, err)
	}
	n.listenAddr = ln.Addr().String()
	n.advertiseAddr = n.listenAddr
	if n.cfg.AdvertiseAddr != "" {
		n.advertiseAddr = n.cfg.AdvertiseAddr
	}
	if host, _, splitErr := net.SplitHostPort(n.advertiseAddr); splitErr == nil {
		if ip := net.ParseIP(host); ip != nil && ip.IsUnspecified() {
			n.logf("AVISO: anunciando endereço wildcard %s ao tracker; peers em outros hosts não conseguirão conectar (use -listen <ip>:<porta> ou -advertise <ip>:<porta>)", n.advertiseAddr)
		}
	}

	context.AfterFunc(n.ctx, func() { ln.Close() })

	n.start = time.Now()
	have, total := n.store.HaveCount()
	role := "peer"
	if n.cfg.Seed {
		role = "semente"
	}
	n.logf("%s ouvindo em %s (anunciando %s) | modo %s | política %s | %s: %d/%d blocos de %d KiB",
		role, n.listenAddr, n.advertiseAddr, n.cfg.Mode, n.picker.Policy.String(), n.m.Name, have, total, n.m.PieceSize>>10)

	n.wg.Add(4)
	go n.acceptLoop(ln)
	go n.announceLoop()
	go n.progressLoop()
	go n.inflightLoop()

	if !n.cfg.Seed && n.store.IsComplete() {
		n.finish()
	}

	stay := n.cfg.Seed || n.cfg.Linger
	select {
	case <-n.ctx.Done():
	case <-n.done:
		if stay {
			<-n.ctx.Done()
		}
	}
	n.shutdown()
	return nil
}

// shutdown encerra o nó de forma ordenada.
func (n *Node) shutdown() {
	n.stop()    // fecha o listener e inicia o encerramento gracioso das conexões
	n.wg.Wait() // espera todas as goroutines antes de fechar o arquivo
	if _, err := n.announce("stopped"); err != nil {
		n.logf("não consegui avisar o tracker da saída: %v", err)
	}
	if !n.cfg.Seed { // usado para medir a disponibilidade quando a semente sai
		have, total := n.store.HaveCount()
		n.logf("bitfield final (%d/%d blocos): %x", have, total, encodeBitfield(n.store.HaveSlice()))
	}
	n.store.Close()
	n.logf("encerrado (enviou %s)", mb(n.uploaded.Load()))
}

// shares diz se este nó repassa blocos aos outros. No modo cliente-servidor
// só a semente (o servidor) envia; os clientes apenas baixam.
func (n *Node) shares() bool {
	return n.cfg.Seed || n.cfg.Mode == ModeP2P
}

// --- tracker e descoberta de vizinhos ---

// announceLoop é o heartbeat no tracker: a cada intervalo avisa que o nó
// continua vivo e recebe a lista atualizada de peers ativos.
func (n *Node) announceLoop() {
	defer n.wg.Done()
	interval := 2 * time.Second
	for {
		resp, err := n.announce("")
		if n.ctx.Err() != nil {
			return
		}
		if err != nil {
			n.logf("tracker indisponível: %v", err)
		} else {
			if resp.Interval > 0 {
				interval = resp.Interval
			}
			n.connectTo(resp.Peers)
		}
		select {
		case <-n.ctx.Done():
			return
		case <-time.After(interval):
		}
	}
}

var trackerClient = &http.Client{Timeout: 3 * time.Second}

// announce manda um anúncio ao tracker (POST /announce) e devolve a resposta.
func (n *Node) announce(event string) (*tracker.AnnounceResponse, error) {
	ctx := n.ctx
	if event == "stopped" {
		ctx = context.Background() // n.ctx já foi cancelado nesse ponto
	}
	body, err := json.Marshal(tracker.AnnounceRequest{
		InfoHash: n.infoHash,
		ID:       n.cfg.ID,
		Addr:     n.advertiseAddr,
		Seed:     n.cfg.Seed,
		Mode:     n.cfg.Mode,
		Event:    event,
	})
	if err != nil {
		return nil, err
	}
	url := strings.TrimRight(n.cfg.TrackerURL, "/") + "/announce"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := trackerClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		return nil, fmt.Errorf("tracker respondeu %s: %s", resp.Status, bytes.TrimSpace(msg))
	}
	var out tracker.AnnounceResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("resposta do tracker inválida: %w", err)
	}
	return &out, nil
}

// connectTo abre conexão com os peers da lista que ainda não são vizinhos.
func (n *Node) connectTo(peers []tracker.PeerInfo) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, p := range peers {
		if p.ID == n.cfg.ID || n.conns[p.ID] != nil || n.dialing[p.ID] || n.banned[p.ID] {
			continue
		}
		if !n.shouldDial(p) {
			continue
		}
		n.dialing[p.ID] = true
		n.wg.Add(1)
		go n.dial(p)
	}
}

// shouldDial aplica as regras de quem abre a conexão:
//
//   - a semente nunca disca: ela só aceita conexões;
//   - todo mundo disca para a semente;
//   - no modo cliente-servidor, o cliente só conversa com a semente;
//   - peers P2P não discam para clientes em modo cliente-servidor;
//   - no P2P, entre dois peers comuns só o de menor id disca, para não
//     existirem duas conexões entre o mesmo par.
func (n *Node) shouldDial(p tracker.PeerInfo) bool {
	switch {
	case n.cfg.Seed:
		return false
	case p.Seed:
		return true
	case n.cfg.Mode == ModeCS:
		return false
	case p.Mode == ModeCS:
		return false
	default:
		return n.cfg.ID < p.ID
	}
}

// --- progresso e resultado ---

func (n *Node) progressLoop() {
	defer n.wg.Done()
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-t.C:
		}
		have, total := n.store.HaveCount()
		pct := 0.0
		if total > 0 {
			pct = 100 * float64(have) / float64(total)
		}
		n.mu.Lock()
		peers := len(n.conns)
		n.mu.Unlock()
		n.logf("%5.1f%% (%d/%d blocos) | enviou %s | %d vizinhos",
			pct, have, total, mb(n.uploaded.Load()), peers)
	}
}

// Result é gravado em Config.ResultPath quando o download termina; os scripts
// do experimento leem esses arquivos para montar os gráficos.
type Result struct {
	PeerID    string           `json:"peer_id"`
	Mode      string           `json:"mode"`
	Picker    string           `json:"picker"`
	StartMs   int64            `json:"start_unix_ms"`
	EndMs     int64            `json:"end_unix_ms"`
	Seconds   float64          `json:"seconds"`
	BytesFrom map[string]int64 `json:"bytes_from"` // de quem veio cada byte
}

// finish roda uma única vez, quando o último bloco é gravado (ou se o arquivo
// já estava completo ao iniciar o download).
func (n *Node) finish() {
	n.finishOnce.Do(func() {
		end := time.Now()
		close(n.done)
		n.logf("DOWNLOAD COMPLETO em %.2fs", end.Sub(n.start).Seconds())
		if n.cfg.ResultPath == "" {
			return
		}
		n.mu.Lock()
		res := Result{
			PeerID:    n.cfg.ID,
			Mode:      n.cfg.Mode,
			Picker:    n.cfg.Picker,
			StartMs:   n.start.UnixMilli(),
			EndMs:     end.UnixMilli(),
			Seconds:   end.Sub(n.start).Seconds(),
			BytesFrom: map[string]int64{},
		}
		for id, b := range n.bytesFrom {
			res.BytesFrom[id] = b
		}
		n.mu.Unlock()
		if err := writeJSONAtomic(n.cfg.ResultPath, res); err != nil {
			n.logf("erro gravando resultado: %v", err)
		}
	})
}

// writeJSONAtomic grava num arquivo temporário e renomeia, para que quem
// estiver esperando o arquivo nunca leia um JSON pela metade.
func writeJSONAtomic(path string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// encodeBitfield empacota have no formato do wire (1 bit por bloco).
func encodeBitfield(have []bool) []byte {
	bf := make([]byte, wire.BitfieldBytes(len(have)))
	for i, h := range have {
		if h {
			wire.SetBit(bf, i, true) // não falha: bf tem o tamanho certo
		}
	}
	return bf
}

func (n *Node) logf(format string, args ...any) { n.log.Printf(format, args...) }

func mb(b int64) string { return fmt.Sprintf("%.1f MB", float64(b)/1e6) }

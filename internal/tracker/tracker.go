package tracker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"time"
)

// PeerInfo representa um peer anunciado no enxame de um infohash.
type PeerInfo struct {
	ID       string `json:"id"`
	Addr     string `json:"addr"`
	Seed     bool   `json:"seed"`
	LastSeen time.Time
}

// AnnounceRequest é o payload enviado pelo peer ao tracker.
type AnnounceRequest struct {
	InfoHash string `json:"infohash"`
	ID       string `json:"id"`
	Addr     string `json:"addr"`
	Seed     bool   `json:"seed"`
	Event    string `json:"event,omitempty"` // "", "started", "stopped"
}

// AnnounceResponse é o que o tracker devolve ao peer.
type AnnounceResponse struct {
	Interval time.Duration `json:"interval"`
	Peers    []PeerInfo    `json:"peers"`
}

// Server mantém os enxames ativos em memória e expõe endpoints HTTP.
type Server struct {
	interval time.Duration
	ttl      time.Duration

	mu     sync.Mutex
	swarms map[string]map[string]PeerInfo // infohash -> peerID -> PeerInfo
}

// NewServer cria um tracker com intervalo de heartbeat e TTL de expiração.
func NewServer(interval, ttl time.Duration) *Server {
	if interval <= 0 {
		interval = 2 * time.Second
	}
	if ttl <= interval {
		ttl = 3 * interval
	}
	return &Server{
		interval: interval,
		ttl:      ttl,
		swarms:   make(map[string]map[string]PeerInfo),
	}
}

// pruneExpiredLocked remove peers sem heartbeat além do TTL.
// Convenção em Go: sufixo `Locked` indica que o chamador já segurou `s.mu`.
func (s *Server) pruneExpiredLocked(now time.Time) {
	for ih, peers := range s.swarms {
		for id, p := range peers {
			if now.Sub(p.LastSeen) > s.ttl {
				delete(peers, id)
			}
		}
		if len(peers) == 0 {
			delete(s.swarms, ih)
		}
	}
}

// Announce atualiza o estado de um peer e retorna os peers ativos do enxame.
func (s *Server) Announce(req AnnounceRequest) ([]PeerInfo, error) {
	if req.InfoHash == "" || req.ID == "" {
		return nil, errors.New("infohash e id são obrigatórios")
	}

	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()

	s.pruneExpiredLocked(now)

	peers, ok := s.swarms[req.InfoHash]
	if !ok {
		if req.Event == "stopped" {
			return []PeerInfo{}, nil
		}
		peers = make(map[string]PeerInfo)
		s.swarms[req.InfoHash] = peers
	}

	if req.Event == "stopped" {
		delete(peers, req.ID)
		if len(peers) == 0 {
			delete(s.swarms, req.InfoHash)
		}
		return []PeerInfo{}, nil
	}

	if req.Addr == "" {
		return nil, errors.New("addr é obrigatório no anúncio")
	}

	peers[req.ID] = PeerInfo{
		ID:       req.ID,
		Addr:     req.Addr,
		Seed:     req.Seed,
		LastSeen: now,
	}

	out := make([]PeerInfo, 0, len(peers))
	for _, p := range peers {
		out = append(out, p)
	}
	return out, nil
}

// SwarmsSnapshot devolve uma cópia segura dos enxames ativos para inspeção.
func (s *Server) SwarmsSnapshot() map[string][]PeerInfo {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()

	s.pruneExpiredLocked(now)

	snap := make(map[string][]PeerInfo, len(s.swarms))
	for ih, peers := range s.swarms {
		list := make([]PeerInfo, 0, len(peers))
		for _, p := range peers {
			list = append(list, p)
		}
		snap[ih] = list
	}
	return snap
}

func (s *Server) handleAnnounce(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodGet {
		http.Error(w, "método não permitido", http.StatusMethodNotAllowed)
		return
	}

	var req AnnounceRequest
	if r.Method == http.MethodPost {
		defer r.Body.Close()
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "JSON inválido", http.StatusBadRequest)
			return
		}
	} else {
		q := r.URL.Query()
		req = AnnounceRequest{
			InfoHash: q.Get("infohash"),
			ID:       q.Get("id"),
			Addr:     q.Get("addr"),
			Seed:     q.Get("seed") == "1" || q.Get("seed") == "true",
			Event:    q.Get("event"),
		}
	}

	peers, err := s.Announce(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(AnnounceResponse{
		Interval: s.interval,
		Peers:    peers,
	})
}

func (s *Server) handleSwarms(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "método não permitido", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(s.SwarmsSnapshot())
}

// Run inicia o servidor HTTP em `addr` e encerra suavemente quando `ctx` é cancelado.
func (s *Server) Run(ctx context.Context, addr string) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/announce", s.handleAnnounce)
	mux.HandleFunc("/swarms", s.handleSwarms)

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("escutar em %s: %w", addr, err)
	}

	srv := &http.Server{Handler: mux}

	errCh := make(chan error, 1)
	go func() {
		err := srv.Serve(ln)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	log.Printf("tracker ouvindo em %s (interval=%s, ttl=%s)", ln.Addr().String(), s.interval, s.ttl)

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
		<-errCh
		return nil
	case err := <-errCh:
		return err
	}
}

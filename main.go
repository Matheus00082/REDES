package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"minitorrent/internal/manifest"
	"minitorrent/internal/peer"
	"minitorrent/internal/tracker"
)

const usage = `uso: minitorrent <comando> [opções]

comandos:
  manifest   gera o manifesto (SHA-256 de cada bloco) de um arquivo
  tracker    servidor de descoberta (lista de peers ativos, com heartbeat)
  seed       semente: compartilha um arquivo completo (no modo cs, é o servidor)
  get        baixa um arquivo; no modo p2p também repassa blocos aos outros

"minitorrent <comando> -h" mostra as opções de cada comando
`

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	switch os.Args[1] {
	case "manifest":
		if err := runManifest(os.Args[2:]); err != nil {
			log.Fatalf("erro: %v", err)
		}
	case "tracker":
		if err := runTracker(ctx, os.Args[2:]); err != nil {
			log.Fatalf("erro: %v", err)
		}
	case "seed", "get":
		if err := runPeer(ctx, os.Args[2:], os.Args[1] == "seed"); err != nil {
			log.Fatalf("erro: %v", err)
		}
	default:
		fmt.Fprintf(os.Stderr, "comando desconhecido %q\n\n%s", os.Args[1], usage)
		os.Exit(2)
	}
}

func runManifest(args []string) error {
	fs := flag.NewFlagSet("manifest", flag.ExitOnError)
	in := fs.String("in", "", "arquivo de entrada (obrigatório)")
	out := fs.String("out", "", "arquivo JSON de saída (padrão: <in>.manifest.json)")
	piece := fs.Int64("piece", manifest.DefaultPieceSize, "tamanho do bloco em bytes")

	if err := fs.Parse(args); err != nil {
		return err
	}
	if *in == "" {
		fs.Usage()
		return errors.New("-in é obrigatório")
	}
	if *out == "" {
		*out = *in + ".manifest.json"
	}

	start := time.Now()
	m, err := manifest.Create(*in, *piece)
	if err != nil {
		return err
	}
	if err := m.Save(*out); err != nil {
		return err
	}

	log.Printf("manifesto salvo em %s (%d bytes, %d blocos de %d KiB, %.3fs)",
		*out, m.Size, m.NumPieces(), m.PieceSize/1024, time.Since(start).Seconds())
	return nil
}

func runTracker(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("tracker", flag.ExitOnError)
	addr := fs.String("listen", ":8000", "endereço HTTP do tracker")
	interval := fs.Duration("interval", 2*time.Second, "intervalo de heartbeat")
	ttl := fs.Duration("ttl", 6*time.Second, "tempo sem heartbeat até remover o peer")
	if err := fs.Parse(args); err != nil {
		return err
	}
	return tracker.NewServer(*interval, *ttl).Run(ctx, *addr)
}

// runPeer roda uma semente (seed) ou um peer que baixa o arquivo (get).
func runPeer(ctx context.Context, args []string, seed bool) error {
	name := "get"
	if seed {
		name = "seed"
	}
	fs := flag.NewFlagSet(name, flag.ExitOnError)
	cfg := peer.Config{Seed: seed}

	manifestPath := fs.String("manifest", "", "manifesto do arquivo (obrigatório)")
	if seed {
		fs.StringVar(&cfg.DataPath, "file", "", "arquivo completo a compartilhar (obrigatório)")
	} else {
		fs.StringVar(&cfg.DataPath, "out", "", "onde salvar o arquivo baixado (obrigatório)")
	}
	fs.StringVar(&cfg.ID, "id", "", "nome do peer nos logs e no tracker (padrão: aleatório)")
	fs.StringVar(&cfg.TrackerURL, "tracker", "http://127.0.0.1:8000", "URL do tracker")
	fs.StringVar(&cfg.ListenAddr, "listen", "127.0.0.1:0", "ip:porta para aceitar conexões (o ip é anunciado ao tracker)")
	fs.Float64Var(&cfg.Corrupt, "corrupt", 0, "probabilidade de corromper cada bloco enviado (simula par malicioso)")
	leaveAfter := fs.Float64("leave-after", 0, "sair depois de enviar X vezes o tamanho do arquivo (0 = nunca)")
	if !seed {
		fs.StringVar(&cfg.Mode, "mode", peer.ModeP2P, "p2p, ou cs (cliente-servidor: só baixa da semente e não repassa nada)")
		fs.StringVar(&cfg.Picker, "picker", peer.PickRarest, "política de blocos: rarest ou sequential")
		fs.BoolVar(&cfg.Linger, "linger", false, "continuar semeando depois de completar o download")
		fs.StringVar(&cfg.ResultPath, "result", "", "arquivo JSON com as estatísticas do download")
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *manifestPath == "" || cfg.DataPath == "" {
		fs.Usage()
		return errors.New("-manifest e o arquivo de dados são obrigatórios")
	}

	m, err := manifest.Load(*manifestPath)
	if err != nil {
		return err
	}
	cfg.LeaveAfter = int64(*leaveAfter * float64(m.Size))

	n, err := peer.New(cfg, m)
	if err != nil {
		return err
	}
	return n.Run(ctx)
}

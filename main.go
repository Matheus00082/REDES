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
	"minitorrent/internal/tracker"
)

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "uso: minitorrent manifest -in <arquivo> [-out <arquivo.json>] [-piece 262144]")
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
	default:
		fmt.Fprintf(os.Stderr, "comando desconhecido %q (use: manifest)\n", os.Args[1])
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

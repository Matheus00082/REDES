package peer

import (
	"bytes"
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"

	"minitorrent/internal/manifest"
)

func TestStorageWriteReadAndResume(t *testing.T) {
	dir := t.TempDir()
	origPath := filepath.Join(dir, "orig.bin")
	outPath := filepath.Join(dir, "out.bin")

	// 1000 bytes com blocos de 300 -> 300, 300, 300, 100
	orig := make([]byte, 1000)
	_, _ = rand.Read(orig)
	if err := os.WriteFile(origPath, orig, 0o644); err != nil {
		t.Fatal(err)
	}

	m, err := manifest.Create(origPath, 300)
	if err != nil {
		t.Fatal(err)
	}

	st, err := OpenStorage(outPath, m, false)
	if err != nil {
		t.Fatal(err)
	}

	// Grava fora de ordem: bloco 2 (600:900) e bloco 3 (900:1000)
	if added, err := st.WritePiece(2, orig[600:900]); err != nil || !added {
		t.Fatalf("esperava gravar bloco 2: added=%v err=%v", added, err)
	}
	if added, err := st.WritePiece(3, orig[900:1000]); err != nil || !added {
		t.Fatalf("esperava gravar bloco 3: added=%v err=%v", added, err)
	}

	// Bloco inválido deve falhar e não marcar `have[0]`
	bad := append([]byte(nil), orig[0:300]...)
	bad[0] ^= 0xff
	if _, err := st.WritePiece(0, bad); err == nil {
		t.Fatal("esperava erro ao gravar bloco com hash inválido")
	}
	if st.HasPiece(0) {
		t.Fatal("bloco 0 não deveria estar marcado como disponível")
	}

	// Fecha e reabre para simular retomada de download
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	st2, err := OpenStorage(outPath, m, false)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()

	have, total := st2.HaveCount()
	if have != 2 || total != 4 {
		t.Fatalf("esperava 2/4 blocos na retomada, obteve %d/%d", have, total)
	}

	got2, err := st2.ReadPiece(2)
	if err != nil || !bytes.Equal(got2, orig[600:900]) {
		t.Fatalf("falha ao ler bloco 2 após retomada: err=%v", err)
	}
}

package manifest

import (
	"crypto/rand"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestCreateVerifyAndLoad(t *testing.T) {
	data := make([]byte, 1000)
	rand.Read(data)
	path := filepath.Join(t.TempDir(), "dados.bin")
	os.WriteFile(path, data, 0o644)

	m, err := Create(path, 300) // blocos de 300 + 300 + 300 + 100 bytes
	if err != nil {
		t.Fatal(err)
	}
	if m.NumPieces() != 4 || m.PieceLen(3) != 100 {
		t.Fatalf("blocos=%d último=%d", m.NumPieces(), m.PieceLen(3))
	}
	if !m.Verify(1, data[300:600]) {
		t.Fatal("bloco correto foi recusado")
	}
	bad := slices.Clone(data[300:600])
	bad[10] ^= 1
	if m.Verify(1, bad) {
		t.Fatal("bloco com 1 bit trocado foi aceito")
	}

	if err := m.Save(path + ".json"); err != nil {
		t.Fatal(err)
	}
	loaded, err := Load(path + ".json")
	if err != nil || loaded.InfoHash() != m.InfoHash() {
		t.Fatalf("o manifesto mudou ao salvar e carregar (err=%v)", err)
	}
}

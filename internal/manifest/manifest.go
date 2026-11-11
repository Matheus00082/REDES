// Package manifest descreve o arquivo compartilhado: nome, tamanho, tamanho
// dos blocos e o SHA-256 de cada bloco. É o equivalente ao arquivo ".torrent".
package manifest

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// DefaultPieceSize é o tamanho padrão de bloco (256 KiB).
const DefaultPieceSize = 256 << 10

// Manifest é salvo em JSON e entregue a todos que vão baixar o arquivo.
type Manifest struct {
	Name      string   `json:"name"`
	Size      int64    `json:"size"`
	PieceSize int64    `json:"piece_size"`
	Pieces    []string `json:"pieces"` // SHA-256 (hex) de cada bloco, em ordem
}

// Create lê o arquivo e calcula o hash de cada bloco.
func Create(path string, pieceSize int64) (*Manifest, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	m := &Manifest{Name: filepath.Base(path), PieceSize: pieceSize}
	buf := make([]byte, pieceSize)
	for {
		n, err := io.ReadFull(f, buf)
		if n > 0 {
			sum := sha256.Sum256(buf[:n])
			m.Pieces = append(m.Pieces, hex.EncodeToString(sum[:]))
			m.Size += int64(n)
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break // fim do arquivo
		}
		if err != nil {
			return nil, err
		}
	}
	if m.Size == 0 {
		return nil, errors.New("arquivo vazio")
	}
	return m, nil
}

// Load lê um manifesto salvo e confere se ele é coerente.
func Load(path string) (*Manifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("manifesto %s: %w", path, err)
	}
	if m.Size <= 0 || m.PieceSize <= 0 || int64(len(m.Pieces)) != (m.Size+m.PieceSize-1)/m.PieceSize {
		return nil, fmt.Errorf("manifesto %s incoerente: %d hashes para %d bytes", path, len(m.Pieces), m.Size)
	}
	return &m, nil
}

// Save grava o manifesto em JSON.
func (m *Manifest) Save(path string) error {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

// NumPieces devolve a quantidade de blocos.
func (m *Manifest) NumPieces() int { return len(m.Pieces) }

// PieceLen devolve o tamanho do bloco i (o último pode ser menor).
func (m *Manifest) PieceLen(i int) int64 {
	if i == len(m.Pieces)-1 {
		return m.Size - int64(i)*m.PieceSize
	}
	return m.PieceSize
}

// Verify diz se data é exatamente o bloco i (tamanho e SHA-256 certos).
func (m *Manifest) Verify(i int, data []byte) bool {
	if int64(len(data)) != m.PieceLen(i) {
		return false
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]) == m.Pieces[i]
}

// InfoHash identifica o arquivo na rede: é o SHA-256 do manifesto em JSON.
// Dois peers só conversam se tiverem o mesmo InfoHash.
func (m *Manifest) InfoHash() [32]byte {
	data, _ := json.Marshal(m) // não falha: o manifesto só tem strings e números
	return sha256.Sum256(data)
}

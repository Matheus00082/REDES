package peer

import (
	"errors"
	"fmt"
	"os"
	"sync"

	"minitorrent/internal/manifest"
)

// Storage gerencia a leitura e escrita aleatória de blocos em um arquivo no disco
// e rastreia quais blocos já estão completos e verificados.
type Storage struct {
	mu       sync.RWMutex
	file     *os.File
	manifest *manifest.Manifest
	have     []bool
	haveCnt  int
}

// OpenStorage abre/cria o arquivo em `path` preparado para o manifesto `m`.
// Se `readOnly` for true (ex.: semente), abre somente para leitura.
// Se `readOnly` for false (ex.: download/get), cria/abre leitura+escrita e escaneia
// blocos já existentes e íntegros para permitir retomada.
func OpenStorage(path string, m *manifest.Manifest, readOnly bool) (*Storage, error) {
	if m == nil {
		return nil, errors.New("manifesto não pode ser nulo")
	}
	if m.PieceSize <= 0 || m.Size < 0 {
		return nil, errors.New("manifesto com tamanho ou piece_size inválido")
	}

	flags := os.O_RDWR | os.O_CREATE
	if readOnly {
		flags = os.O_RDONLY
	}

	f, err := os.OpenFile(path, flags, 0o644)
	if err != nil {
		return nil, fmt.Errorf("abrir arquivo de dados %q: %w", path, err)
	}

	if !readOnly {
		// Garante que o arquivo tenha o tamanho final esperado (útil para gravações em qualquer offset).
		if err := f.Truncate(m.Size); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("ajustar tamanho do arquivo: %w", err)
		}
	}

	s := &Storage{
		file:     f,
		manifest: m,
		have:     make([]bool, m.NumPieces()),
	}

	if err := s.scanExisting(); err != nil {
		_ = f.Close()
		return nil, err
	}

	return s, nil
}

// PieceSizeAt retorna o tamanho esperado (em bytes) do bloco `index`.
func (s *Storage) PieceSizeAt(index int) (int, error) {
	n := s.manifest.NumPieces()
	if index < 0 || index >= n {
		return 0, fmt.Errorf("índice de bloco fora do intervalo: %d", index)
	}
	offset := int64(index) * s.manifest.PieceSize
	rem := s.manifest.Size - offset
	if rem < s.manifest.PieceSize {
		return int(rem), nil
	}
	return int(s.manifest.PieceSize), nil
}

// scanExisting verifica quais blocos no arquivo já batem com o hash do manifesto.
func (s *Storage) scanExisting() error {
	n := s.manifest.NumPieces()
	cnt := 0
	for i := 0; i < n; i++ {
		sz, err := s.PieceSizeAt(i)
		if err != nil {
			return err
		}
		if sz == 0 {
			continue
		}
		buf := make([]byte, sz)
		offset := int64(i) * s.manifest.PieceSize
		_, err = s.file.ReadAt(buf, offset)
		if err != nil {
			// Arquivo novo ou menor que o offset ainda não tem o bloco válido.
			continue
		}
		if s.manifest.Verify(i, buf) {
			s.have[i] = true
			cnt++
		}
	}
	s.haveCnt = cnt
	return nil
}

// ReadPiece lê o bloco `index` do disco.
func (s *Storage) ReadPiece(index int) ([]byte, error) {
	sz, err := s.PieceSizeAt(index)
	if err != nil {
		return nil, err
	}

	s.mu.RLock()
	has := s.have[index]
	s.mu.RUnlock()
	if !has {
		return nil, fmt.Errorf("bloco %d não disponível localmente", index)
	}

	buf := make([]byte, sz)
	offset := int64(index) * s.manifest.PieceSize
	if _, err := s.file.ReadAt(buf, offset); err != nil {
		return nil, fmt.Errorf("ler bloco %d do disco: %w", index, err)
	}
	return buf, nil
}

// WritePiece valida o hash do bloco `index`, grava no offset correto do arquivo
// e marca o bloco como disponível. Retorna (true, nil) se foi um bloco novo aceito,
// ou (false, nil) se já estava completo.
func (s *Storage) WritePiece(index int, data []byte) (bool, error) {
	sz, err := s.PieceSizeAt(index)
	if err != nil {
		return false, err
	}
	if len(data) != sz {
		return false, fmt.Errorf("tamanho inválido para bloco %d: esperava %d, recebeu %d", index, sz, len(data))
	}
	if !s.manifest.Verify(index, data) {
		return false, fmt.Errorf("hash SHA-256 inválido para bloco %d", index)
	}

	offset := int64(index) * s.manifest.PieceSize
	if _, err := s.file.WriteAt(data, offset); err != nil {
		return false, fmt.Errorf("gravar bloco %d no disco: %w", index, err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.have[index] {
		return false, nil
	}
	s.have[index] = true
	s.haveCnt++
	return true, nil
}

// HasPiece informa se o nó já possui o bloco `index` validado.
func (s *Storage) HasPiece(index int) bool {
	if index < 0 || index >= s.manifest.NumPieces() {
		return false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.have[index]
}

// HaveCount retorna quantos blocos válidos o nó possui e o total de blocos.
func (s *Storage) HaveCount() (have int, total int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.haveCnt, s.manifest.NumPieces()
}

// IsComplete informa se todos os blocos do manifesto já estão presentes.
func (s *Storage) IsComplete() bool {
	have, total := s.HaveCount()
	return total > 0 && have == total
}

// BitfieldBytes retorna uma cópia do vetor de bits representando os blocos que o nó possui.
func (s *Storage) HaveSlice() []bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]bool, len(s.have))
	copy(out, s.have)
	return out
}

// Close fecha o arquivo subjacente.
func (s *Storage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file == nil {
		return nil
	}
	err := s.file.Close()
	s.file = nil
	return err
}

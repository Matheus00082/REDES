package wire

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	MsgKeepAlive uint8 = 0
	MsgHave      uint8 = 4
	MsgBitfield  uint8 = 5
	MsgRequest   uint8 = 6
	MsgPiece     uint8 = 7

	// MaxFrameSize evita que um quadro peça alocação gigante de memória.
	MaxFrameSize uint32 = 16 * 1024 * 1024

	// MaxPieceSize é o maior bloco que cabe em um quadro MsgPiece
	// (MaxFrameSize menos 1 byte de tipo e 4 bytes do índice do bloco).
	MaxPieceSize int64 = int64(MaxFrameSize) - 5
)

// Frame representa uma mensagem decodificada do protocolo sobre TCP.
type Frame struct {
	Type    uint8
	Payload []byte
}

// WriteFrame escreve [len uint32 (1 + len(payload))][type uint8][payload] em um quadro binário (w).
func WriteFrame(w io.Writer, msgType uint8, payload []byte) error {
	totalLen64 := int64(1) + int64(len(payload))
	if totalLen64 > int64(MaxFrameSize) {
		return fmt.Errorf("quadro muito grande: %d bytes (máximo %d)", totalLen64, MaxFrameSize)
	}

	var hdr [5]byte
	binary.BigEndian.PutUint32(hdr[:4], uint32(totalLen64))
	hdr[4] = msgType

	if _, err := w.Write(hdr[:]); err != nil {
		return fmt.Errorf("escrever cabeçalho do quadro: %w", err)
	}

	if len(payload) > 0 {
		if _, err := w.Write(payload); err != nil {
			return fmt.Errorf("escrever payload do quadro: %w", err)
		}
	}

	return nil
}

// ReadFrame lê exatamente um quadro completo de `r`.
func ReadFrame(r io.Reader) (Frame, error) {
	var hdr [5]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return Frame{}, err
	}

	totalLen := binary.BigEndian.Uint32(hdr[:4])
	msgType := hdr[4]

	if totalLen == 0 || totalLen > MaxFrameSize {
		return Frame{}, fmt.Errorf("tamanho de quadro inválido: %d", totalLen)
	}

	payloadLen := int(totalLen - 1)
	var payload []byte
	if payloadLen > 0 {
		payload = make([]byte, payloadLen)
		if _, err := io.ReadFull(r, payload); err != nil {
			return Frame{}, fmt.Errorf("ler payload do quadro: %w", err)
		}
	}

	return Frame{
		Type:    msgType,
		Payload: payload,
	}, nil
}

// --- Helpers para mensagens específicas ---

func WriteKeepAlive(w io.Writer) error {
	return WriteFrame(w, MsgKeepAlive, nil)
}

func EncodeUint32(v uint32) []byte {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], v)
	return b[:]
}

func DecodeUint32(b []byte) (uint32, error) {
	if len(b) != 4 {
		return 0, fmt.Errorf("esperava 4 bytes para uint32, veio %d", len(b))
	}
	return binary.BigEndian.Uint32(b), nil
}

func WriteHave(w io.Writer, pieceIndex uint32) error {
	return WriteFrame(w, MsgHave, EncodeUint32(pieceIndex))
}

func WriteRequest(w io.Writer, pieceIndex uint32) error {
	return WriteFrame(w, MsgRequest, EncodeUint32(pieceIndex))
}

func WritePiece(w io.Writer, pieceIndex uint32, data []byte) error {
	buf := make([]byte, 4+len(data))
	binary.BigEndian.PutUint32(buf[:4], pieceIndex)
	copy(buf[4:], data)
	return WriteFrame(w, MsgPiece, buf)
}

func DecodePiece(payload []byte) (pieceIndex uint32, data []byte, err error) {
	if len(payload) < 4 {
		return 0, nil, fmt.Errorf("payload de PIECE curto demais: %d bytes", len(payload))
	}
	pieceIndex = binary.BigEndian.Uint32(payload[:4])
	return pieceIndex, payload[4:], nil
}

// --- Bitfield (1 bit por bloco) ---

// BitfieldBytes retorna o número de bytes necessários para representar `numPieces` bits.
func BitfieldBytes(numPieces int) int {
	if numPieces <= 0 {
		return 0
	}
	return (numPieces + 7) / 8
}

// HasBit verifica se o bit `idx` está ligado no bitfield.
func HasBit(bf []byte, idx int) bool {
	if idx < 0 {
		return false
	}
	byteIdx := idx / 8
	bitOffset := uint(7 - (idx % 8)) // MSB primeiro
	if byteIdx >= len(bf) {
		return false
	}
	return (bf[byteIdx] & (1 << bitOffset)) != 0
}

// SetBit liga ou desliga o bit `idx` no bitfield.
func SetBit(bf []byte, idx int, val bool) error {
	if idx < 0 {
		return errors.New("índice de bit negativo")
	}
	byteIdx := idx / 8
	bitOffset := uint(7 - (idx % 8))
	if byteIdx >= len(bf) {
		return fmt.Errorf("índice de bit %d fora do bitfield de %d bytes", idx, len(bf))
	}
	if val {
		bf[byteIdx] |= 1 << bitOffset
	} else {
		bf[byteIdx] &^= 1 << bitOffset // em Go, `&^` limpa os bits setados à direita
	}
	return nil
}

// Handshake troca e valida infohash e peerID sobre uma conexão io.ReadWriter.
func Handshake(rw io.ReadWriter, localInfoHash, localPeerID string) (remotePeerID string, err error) {
	if len(localInfoHash) == 0 || len(localInfoHash) > 4096 {
		return "", errors.New("infohash local inválido")
	}
	if len(localPeerID) == 0 || len(localPeerID) > 65535 {
		return "", errors.New("peerID local inválido")
	}

	// Formato simples: [len(infohash) uint16][infohash][len(peerID) uint16][peerID]
	ihBytes := []byte(localInfoHash)
	idBytes := []byte(localPeerID)

	out := make([]byte, 2+len(ihBytes)+2+len(idBytes))
	pos := 0
	binary.BigEndian.PutUint16(out[pos:pos+2], uint16(len(ihBytes)))
	pos += 2
	copy(out[pos:pos+len(ihBytes)], ihBytes)
	pos += len(ihBytes)

	binary.BigEndian.PutUint16(out[pos:pos+2], uint16(len(idBytes)))
	pos += 2
	copy(out[pos:], idBytes)

	if _, err := rw.Write(out); err != nil {
		return "", fmt.Errorf("enviar handshake: %w", err)
	}

	// Lê infohash remoto
	var lenBuf [2]byte
	if _, err := io.ReadFull(rw, lenBuf[:]); err != nil {
		return "", fmt.Errorf("ler tamanho do infohash remoto: %w", err)
	}
	remIHLen := int(binary.BigEndian.Uint16(lenBuf[:]))
	if remIHLen <= 0 || remIHLen > 4096 {
		return "", fmt.Errorf("tamanho de infohash remoto inválido: %d", remIHLen)
	}
	remIH := make([]byte, remIHLen)
	if _, err := io.ReadFull(rw, remIH); err != nil {
		return "", fmt.Errorf("ler infohash remoto: %w", err)
	}
	if string(remIH) != localInfoHash {
		return "", errors.New("infohash incompatível no handshake")
	}

	// Lê peerID remoto
	if _, err := io.ReadFull(rw, lenBuf[:]); err != nil {
		return "", fmt.Errorf("ler tamanho do peerID remoto: %w", err)
	}
	remIDLen := int(binary.BigEndian.Uint16(lenBuf[:]))
	if remIDLen <= 0 || remIDLen > 65535 {
		return "", fmt.Errorf("tamanho de peerID remoto inválido: %d", remIDLen)
	}
	remID := make([]byte, remIDLen)
	if _, err := io.ReadFull(rw, remID); err != nil {
		return "", fmt.Errorf("ler peerID remoto: %w", err)
	}

	return string(remID), nil
}

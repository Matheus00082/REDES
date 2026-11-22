package wire

import (
	"bytes"
	"net"
	"testing"
)

func TestFramesAndBitfield(t *testing.T) {
	var buf bytes.Buffer

	if err := WriteKeepAlive(&buf); err != nil {
		t.Fatal(err)
	}
	if err := WriteHave(&buf, 42); err != nil {
		t.Fatal(err)
	}
	if err := WriteRequest(&buf, 7); err != nil {
		t.Fatal(err)
	}
	if err := WritePiece(&buf, 7, []byte("conteudo-do-bloco")); err != nil {
		t.Fatal(err)
	}

	// 1. Verifica a leitura de uma mensagem KeepAlive
	f1, err := ReadFrame(&buf)
	if err != nil || f1.Type != MsgKeepAlive || len(f1.Payload) != 0 {
		t.Fatalf("esperava KeepAlive, obteve %+v err=%v", f1, err)
	}

	// 2. Valida a mensagem Have e decodifica o índice do bloco anunciado (bloco 42)
	f2, err := ReadFrame(&buf)
	if err != nil || f2.Type != MsgHave {
		t.Fatalf("esperava Have, obteve %+v err=%v", f2, err)
	}
	idx, err := DecodeUint32(f2.Payload)
	if err != nil || idx != 42 {
		t.Fatalf("esperava bloco 42, obteve %d err=%v", idx, err)
	}

	// 3. Confirma o recebimento de uma mensagem Request válida
	f3, err := ReadFrame(&buf)
	if err != nil || f3.Type != MsgRequest {
		t.Fatalf("esperava Request, obteve %+v err=%v", f3, err)
	}

	// 4. Verifica a mensagem Piece e extrai o índice (7) e os dados do bloco
	f4, err := ReadFrame(&buf)
	if err != nil || f4.Type != MsgPiece {
		t.Fatalf("esperava Piece, obteve %+v err=%v", f4, err)
	}
	pIdx, pData, err := DecodePiece(f4.Payload)
	if err != nil || pIdx != 7 || string(pData) != "conteudo-do-bloco" {
		t.Fatalf("piece inválido: idx=%d data=%q err=%v", pIdx, string(pData), err)
	}

	// Bitfield de 10 blocos -> 2 bytes
	bf := make([]byte, BitfieldBytes(10))
	if len(bf) != 2 {
		t.Fatalf("esperava 2 bytes de bitfield, obteve %d", len(bf))
	}
	_ = SetBit(bf, 0, true)
	_ = SetBit(bf, 9, true)
	if !HasBit(bf, 0) || !HasBit(bf, 9) || HasBit(bf, 1) {
		t.Fatalf("falha na leitura/escrita de bits")
	}
	_ = SetBit(bf, 9, false)
	if HasBit(bf, 9) {
		t.Fatalf("bit 9 deveria ter sido limpo")
	}
}

// tcpPair devolve as duas pontas de uma conexão TCP de verdade em 127.0.0.1.
// net.Pipe não serve para testar o handshake: ele não tem buffer, e como os
// dois lados escrevem antes de ler, os dois ficariam travados no Write.
func tcpPair(t *testing.T) (net.Conn, net.Conn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	c1, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	c2, err := ln.Accept()
	if err != nil {
		c1.Close()
		t.Fatal(err)
	}
	return c1, c2
}

func TestHandshake(t *testing.T) {
	c1, c2 := tcpPair(t)
	defer c1.Close()
	defer c2.Close()

	var id1, id2 string
	var err1, err2 error

	done := make(chan struct{})
	go func() {
		id1, err1 = Handshake(c1, "hash123", "peerA")
		close(done)
	}()

	id2, err2 = Handshake(c2, "hash123", "peerB")
	<-done

	if err1 != nil || err2 != nil {
		t.Fatalf("handshake falhou: err1=%v err2=%v", err1, err2)
	}
	if id1 != "peerB" || id2 != "peerA" {
		t.Fatalf("IDs trocados incorretamente: id1=%s id2=%s", id1, id2)
	}
}

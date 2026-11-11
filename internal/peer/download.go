package peer

import (
	"errors"
	"fmt"
	"math/rand"

	"minitorrent/internal/wire"
)

// handleMessage trata uma mensagem recebida do vizinho c. Devolver erro
// significa violação de protocolo: a conexão é encerrada.
func (n *Node) handleMessage(c *Conn, typ byte, payload []byte) error {
	switch typ {
	case wire.MsgKeepAlive:
		return nil // só serve para mostrar que o vizinho está vivo

	case wire.MsgBitfield:
		bits, err := wire.DecodeBitfield(payload, n.m.NumPieces())
		if err != nil {
			return err
		}
		n.mu.Lock()
		for i, has := range bits {
			if has {
				n.markHas(c, i)
			}
		}
		n.mu.Unlock()
		n.requestMore(c)

	case wire.MsgHave:
		i, _, err := n.parseIndex(payload)
		if err != nil {
			return err
		}
		n.mu.Lock()
		n.markHas(c, i)
		n.mu.Unlock()
		n.requestMore(c)

	case wire.MsgRequest:
		i, _, err := n.parseIndex(payload)
		if err != nil {
			return err
		}
		n.mu.Lock()
		has := n.have[i]
		n.mu.Unlock()
		if !has {
			return fmt.Errorf("pediu o bloco %d, que não temos", i)
		}
		select {
		case c.uploads <- i: // writeLoop envia quando puder
		default:
			return errors.New("pedidos demais em aberto")
		}

	case wire.MsgPiece:
		i, data, err := n.parseIndex(payload)
		if err != nil {
			return err
		}
		return n.onPiece(c, i, data)

	default:
		return fmt.Errorf("tipo de mensagem desconhecido: %d", typ)
	}
	return nil
}

func (n *Node) parseIndex(p []byte) (int, []byte, error) {
	i, rest, err := wire.ParseIndex(p)
	if err == nil && (i < 0 || i >= n.m.NumPieces()) {
		err = fmt.Errorf("índice de bloco fora do intervalo: %d", i)
	}
	return i, rest, err
}

// markHas registra que o vizinho c tem o bloco i. Chamar com n.mu travado.
func (n *Node) markHas(c *Conn, i int) {
	if !c.has[i] {
		c.has[i] = true
		n.avail[i]++
	}
}

// onPiece trata um bloco recebido: confere o SHA-256, grava no disco e avisa
// os vizinhos. Bloco corrompido conta uma falta para quem mandou; com
// maxStrikes faltas o vizinho é banido.
func (n *Node) onPiece(c *Conn, i int, data []byte) error {
	valid := n.m.Verify(i, data)

	n.mu.Lock()
	if n.inflight[i] != c {
		n.mu.Unlock()
		return nil // não pedimos esse bloco a esse vizinho: ignora
	}
	if !valid {
		delete(n.inflight, i) // o bloco volta a ser candidato, para qualquer vizinho
		c.pending--
		c.strikes++
		strikes := c.strikes
		if strikes >= maxStrikes {
			n.banned[c.id] = true
		}
		n.mu.Unlock()
		n.logf("bloco %d vindo de %s falhou no SHA-256 (falta %d de %d)", i, c.id, strikes, maxStrikes)
		if strikes >= maxStrikes {
			return fmt.Errorf("%s banido por mandar %d blocos corrompidos", c.id, strikes)
		}
		n.requestAll()
		return nil
	}
	n.mu.Unlock()

	// Grava fora do lock (disco é lento). inflight[i] continua apontando para
	// c, então nenhum outro vizinho recebe pedido desse bloco enquanto isso.
	if _, err := n.file.WriteAt(data, int64(i)*n.m.PieceSize); err != nil {
		n.logf("erro gravando o bloco %d: %v (encerrando)", i, err)
		n.stop()
		return err
	}

	n.mu.Lock()
	delete(n.inflight, i)
	c.pending--
	n.have[i] = true
	n.haveCount++
	n.bytesFrom[c.id] += int64(len(data))
	complete := n.haveCount == len(n.have)
	for _, other := range n.conns {
		other.send(wire.MsgHave, wire.Index(i))
	}
	n.mu.Unlock()

	if complete {
		n.finish()
	} else {
		n.requestMore(c)
	}
	return nil
}

// sendPiece lê o bloco i do disco e o envia ao vizinho c. Chamado só pela
// writeLoop de c, então é a única escrita no socket naquele momento.
func (n *Node) sendPiece(c *Conn, i int) error {
	data := make([]byte, n.m.PieceLen(i))
	if _, err := n.file.ReadAt(data, int64(i)*n.m.PieceSize); err != nil {
		return err
	}
	if rand.Float64() < n.cfg.Corrupt {
		data[0] ^= 0xFF // par malicioso: estraga o bloco antes de mandar
	}
	if err := wire.Write(c.nc, wire.MsgPiece, wire.Index(i), data); err != nil {
		return err
	}
	total := n.uploaded.Add(int64(len(data)))
	if n.cfg.LeaveAfter > 0 && total >= n.cfg.LeaveAfter {
		n.logf("já enviei %s: saindo da rede (-leave-after)", mb(total))
		n.stop()
	}
	return nil
}

// requestMore mantém até `pipeline` pedidos em aberto com o vizinho c, para
// que o enlace nunca fique parado esperando o próximo pedido.
func (n *Node) requestMore(c *Conn) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.conns[c.id] != c || n.haveCount == len(n.have) {
		return // conexão já removida, ou não falta nada
	}
	// Candidato: bloco que não temos, que ninguém está trazendo e que c tem.
	candidate := func(i int) bool { return !n.have[i] && n.inflight[i] == nil && c.has[i] }
	for c.pending < pipeline {
		var i int
		if n.cfg.Picker == "sequential" {
			i = pickSequential(len(n.have), candidate)
		} else {
			i = pickRarest(n.avail, candidate)
		}
		if i < 0 {
			return // c não tem nada que nos sirva agora
		}
		n.inflight[i] = c
		c.pending++
		c.send(wire.MsgRequest, wire.Index(i))
	}
}

// requestAll chama requestMore para todos os vizinhos. Usado quando blocos
// voltam a ficar livres para pedido (vizinho caiu ou mandou bloco corrompido).
func (n *Node) requestAll() {
	n.mu.Lock()
	conns := make([]*Conn, 0, len(n.conns))
	for _, c := range n.conns {
		conns = append(conns, c)
	}
	n.mu.Unlock()
	for _, c := range conns {
		n.requestMore(c)
	}
}

// pickSequential devolve o menor índice i com ok(i), ou -1 se não houver.
func pickSequential(n int, ok func(int) bool) int {
	for i := 0; i < n; i++ {
		if ok(i) {
			return i
		}
	}
	return -1
}

// pickRarest (incremento "mais raro primeiro") devolve, entre os índices com
// ok(i), um dos que estão em menos vizinhos (menor avail[i]), sorteando entre
// os empatados. Devolve -1 se não houver candidato.
//
// O sorteio importa: no começo todos os blocos só existem na semente
// (disponibilidade 1) e, sem ele, todos os peers pediriam os mesmos blocos.
func pickRarest(avail []int, ok func(int) bool) int {
	best, ties := -1, 0
	for i, a := range avail {
		if !ok(i) {
			continue
		}
		switch {
		case best < 0 || a < avail[best]:
			best, ties = i, 1
		case a == avail[best]:
			ties++
			if rand.Intn(ties) == 0 { // cada empatado tem a mesma chance
				best = i
			}
		}
	}
	return best
}

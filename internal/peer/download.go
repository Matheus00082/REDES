//	download.go  download paralelo, escolha de blocos (rarest-first), SHA-256 e banimento

package peer

import (
	"fmt"
	"math/rand"

	"minitorrent/internal/wire"
)

// onBitfield registra os blocos que o vizinho c anunciou ao conectar e já
// pede o que nos interessa.
func (n *Node) onBitfield(c *Conn, bf []byte) error {
	np := n.m.NumPieces()
	if len(bf) != wire.BitfieldBytes(np) {
		return fmt.Errorf("bitfield com %d bytes, esperado %d", len(bf), wire.BitfieldBytes(np))
	}
	n.mu.Lock()
	for i := 0; i < np; i++ {
		if wire.HasBit(bf, i) {
			n.markHasLocked(c, i)
		}
	}
	n.mu.Unlock()
	n.requestMore(c)
	return nil
}

// onHave registra que o vizinho c acabou de obter o bloco i.
func (n *Node) onHave(c *Conn, i int) {
	n.mu.Lock()
	n.markHasLocked(c, i)
	n.mu.Unlock()
	n.requestMore(c)
}

// markHasLocked marca que c tem o bloco i e atualiza a disponibilidade usada
// pelo rarest-first. O chamador precisa estar com n.mu travado.
func (n *Node) markHasLocked(c *Conn, i int) {
	if !c.has[i] {
		c.has[i] = true
		n.avail[i]++
	}
}

// onPiece trata um bloco recebido: confere o SHA-256 contra o manifesto ANTES
// de gravar, grava no disco e avisa os vizinhos. Bloco corrompido conta uma
// falta para quem mandou; com maxStrikes faltas o vizinho é banido.
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
	if _, err := n.store.WritePiece(i, data); err != nil {
		n.logf("erro gravando o bloco %d: %v (encerrando)", i, err)
		n.stop()
		return err
	}

	n.mu.Lock()
	delete(n.inflight, i)
	c.pending--
	n.bytesFrom[c.id] += int64(len(data))
	if n.shares() {
		for _, other := range n.conns {
			other.send(wire.MsgHave, wire.EncodeUint32(uint32(i)))
		}
	}
	n.mu.Unlock()

	if n.store.IsComplete() {
		n.finish()
		return nil
	}
	n.requestMore(c)
	return nil
}

// requestMore mantém até `pipeline` pedidos em aberto com o vizinho c, para
// que o enlace nunca fique parado esperando o próximo pedido. É isso que faz
// o download ser paralelo: cada vizinho recebe pedidos de blocos diferentes.
func (n *Node) requestMore(c *Conn) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.conns[c.id] != c || c.pending >= pipeline || n.store.IsComplete() {
		return // conexão já removida, pipeline cheio, ou não falta nada
	}
	have := n.store.HaveSlice()
	// Candidato: bloco que não temos, que ninguém está trazendo e que c tem.
	candidate := func(i int) bool { return !have[i] && n.inflight[i] == nil && c.has[i] }
	for c.pending < pipeline {
		var i int
		if n.cfg.Picker == PickSequential {
			i = pickSequential(len(have), candidate)
		} else {
			i = pickRarest(n.avail, candidate)
		}
		if i < 0 {
			return // c não tem nada que nos sirva agora
		}
		n.inflight[i] = c
		c.pending++
		c.send(wire.MsgRequest, wire.EncodeUint32(uint32(i)))
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

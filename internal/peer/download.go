//	download.go  download paralelo, controle de inflight/timeouts, SHA-256 e banimento

package peer

import (
	"fmt"
	"time"

	"minitorrent/internal/wire"
)

// inflightReq acompanha um bloco solicitado a um vizinho específico.
type inflightReq struct {
	conn        *Conn
	requestedAt time.Time
	writing     bool // true enquanto o bloco válido está sendo gravado em disco fora do lock
}

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
// pelo Picker (rarest-first). O chamador precisa estar com n.mu travado.
func (n *Node) markHasLocked(c *Conn, i int) {
	if !c.has[i] {
		c.has[i] = true
		n.avail[i]++
	}
}

// onPiece trata um bloco recebido: confere o SHA-256 contra o manifesto ANTES
// de gravar, grava no disco sem recalcular o hash e avisa os vizinhos.
// Bloco corrompido conta uma falta para quem mandou; com maxStrikes faltas
// o vizinho é banido.
func (n *Node) onPiece(c *Conn, i int, data []byte) error {
	valid := n.m.Verify(i, data)

	n.mu.Lock()
	req, ok := n.inflight[i]
	if !ok || req.conn != c || req.writing {
		n.mu.Unlock()
		return nil // não pedimos esse bloco a esse vizinho (ou já está sendo gravado): ignora
	}
	if !valid {
		delete(n.inflight, i) // o bloco volta a ser candidato, para qualquer vizinho
		if c.pending > 0 {
			c.pending--
		}
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
		n.requestAllPrioritizing(c)
		return nil
	}
	// Marca como em gravação para impedir que o timeout de inflight libere o bloco
	// enquanto gravamos no disco fora do lock.
	req.writing = true
	n.inflight[i] = req
	c.timeouts = 0 // vizinho respondeu com bloco válido
	n.mu.Unlock()

	// Grava fora do lock (disco é lento), reaproveitando o SHA-256 já validado acima.
	if _, err := n.store.WriteVerifiedPiece(i, data); err != nil {
		n.mu.Lock()
		if cur, exists := n.inflight[i]; exists && cur.conn == c {
			delete(n.inflight, i)
			if c.pending > 0 {
				c.pending--
			}
		}
		n.mu.Unlock()
		n.logf("erro gravando o bloco %d: %v (encerrando)", i, err)
		n.stop()
		return err
	}

	n.mu.Lock()
	if cur, exists := n.inflight[i]; exists && cur.conn == c {
		delete(n.inflight, i)
		if c.pending > 0 {
			c.pending--
		}
	}
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
// que o enlace nunca fique parado esperando o próximo pedido. A escolha dos
// blocos é delegada ao Picker (picker.go).
func (n *Node) requestMore(c *Conn) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.conns[c.id] != c || c.pending >= pipeline || n.store.IsComplete() {
		return // conexão já removida, pipeline cheio, ou não falta nada
	}
	have := n.store.HaveSlice()
	// Candidato: bloco que não temos, que ninguém está trazendo e que c tem.
	candidate := func(i int) bool {
		_, busy := n.inflight[i]
		return !have[i] && !busy && c.has[i]
	}
	now := time.Now()
	for c.pending < pipeline {
		i := n.picker.PickMatching(n.avail, candidate)
		if i < 0 {
			return // c não tem nada que nos sirva agora
		}
		n.inflight[i] = inflightReq{
			conn:        c,
			requestedAt: now,
		}
		c.pending++
		c.send(wire.MsgRequest, wire.EncodeUint32(uint32(i)))
	}
}

// requestAll chama requestMore para todos os vizinhos. Usado quando blocos
// voltam a ficar livres para pedido (ex.: vizinho caiu).
func (n *Node) requestAll() {
	n.requestAllPrioritizing(nil)
}

// requestAllPrioritizing distribui pedidos entre todos os vizinhos, deixando
// `deprioritized` (ex.: vizinho que acabou de dar timeout ou erro) por último.
func (n *Node) requestAllPrioritizing(deprioritized *Conn) {
	n.mu.Lock()
	conns := make([]*Conn, 0, len(n.conns))
	var last *Conn
	for _, c := range n.conns {
		if c == deprioritized {
			last = c
			continue
		}
		conns = append(conns, c)
	}
	if last != nil {
		conns = append(conns, last)
	}
	n.mu.Unlock()

	for _, c := range conns {
		n.requestMore(c)
	}
}

// inflightLoop verifica periodicamente pedidos sem resposta, evitando que o
// download trave caso um vizinho continue enviando keep-alive mas pare de
// entregar os blocos pedidos.
func (n *Node) inflightLoop() {
	defer n.wg.Done()
	t := time.NewTicker(inflightCheckEvery)
	defer t.Stop()
	for {
		select {
		case <-n.ctx.Done():
			return
		case now := <-t.C:
			n.reapStaleInflight(now)
		}
	}
}

func (n *Node) reapStaleInflight(now time.Time) {
	var (
		released      int
		stalledConns  []*Conn
		deprioritized *Conn
	)

	n.mu.Lock()
	stalledSeen := map[*Conn]bool{}
	for i, req := range n.inflight {
		if req.writing || now.Sub(req.requestedAt) <= requestTimeout {
			continue
		}
		delete(n.inflight, i)
		if req.conn.pending > 0 {
			req.conn.pending--
		}
		req.conn.timeouts++
		released++
		deprioritized = req.conn
		n.logf("timeout esperando bloco %d de %s (timeout %d de %d)", i, req.conn.id, req.conn.timeouts, maxTimeouts)
		if req.conn.timeouts >= maxTimeouts && !stalledSeen[req.conn] {
			stalledSeen[req.conn] = true
			stalledConns = append(stalledConns, req.conn)
		}
	}
	n.mu.Unlock()

	for _, c := range stalledConns {
		n.logf("desconectando %s após %d timeouts de blocos sem resposta", c.id, maxTimeouts)
		c.close()
	}
	if released > 0 {
		n.requestAllPrioritizing(deprioritized)
	}
}

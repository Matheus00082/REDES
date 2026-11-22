//	conn.go      conexões TCP: abertura, handshake, leitura/escrita de mensagens, limpeza

package peer

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"sync"
	"time"

	"minitorrent/internal/tracker"
	"minitorrent/internal/wire"
)

const (
	// Limites superiores para as filas por conexão, evitando crescimento linear
	// ilimitado de memória em arquivos com dezenas de milhares de blocos.
	maxOutQueue    = 4096
	maxUploadQueue = 256
)

// Conn é a conexão TCP com um vizinho. Uma goroutine lê (readLoop) e outra
// escreve (writeLoop). Quem quiser mandar algo usa send, que nunca bloqueia:
// assim uma conexão lenta não trava o resto do nó.
type Conn struct {
	n       *Node
	id      string
	nc      net.Conn
	ctx     context.Context    // cancelado quando a conexão é fechada (c.close)
	cancel  context.CancelFunc // libera esperas do rate limiter imediatamente ao fechar
	out     chan outMsg        // mensagens pequenas a enviar (BITFIELD, HAVE, REQUEST)
	uploads chan int           // blocos que o vizinho pediu e ainda vamos enviar
	closed  chan struct{}      // fechado quando a conexão termina
	once    sync.Once

	// protegidos por Node.mu
	has      []bool // blocos que o vizinho tem
	pending  int    // pedidos nossos a ele ainda sem resposta
	strikes  int    // blocos corrompidos que ele nos mandou
	timeouts int    // timeouts consecutivos de pedidos sem resposta
	// quando ele nos entregou o último bloco válido
	lastPiece time.Time
}

type outMsg struct {
	typ     uint8
	payload []byte
}

// --- abertura de conexões ---

// dial abre uma conexão com o peer p (chamado por connectTo, em node.go).
func (n *Node) dial(p tracker.PeerInfo) {
	defer n.wg.Done()
	defer func() {
		n.mu.Lock()
		delete(n.dialing, p.ID)
		n.mu.Unlock()
	}()
	d := net.Dialer{Timeout: dialTimeout}
	nc, err := d.DialContext(n.ctx, "tcp", p.Addr)
	if err != nil {
		if n.ctx.Err() == nil {
			n.logf("não consegui conectar em %s (%s): %v", p.ID, p.Addr, err)
		}
		return
	}
	n.handle(nc, true)
}

// acceptLoop aceita as conexões que outros peers abrem conosco.
func (n *Node) acceptLoop(ln net.Listener) {
	defer n.wg.Done()
	for {
		nc, err := ln.Accept()
		if err != nil {
			if n.ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return // o nó está encerrando
			}
			n.logf("erro no accept: %v", err)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		n.wg.Add(1)
		go func() {
			defer n.wg.Done()
			n.handle(nc, false)
		}()
	}
}

// --- ciclo de vida de uma conexão ---

// handle conduz uma conexão, aberta por nós (outgoing) ou aceita, do
// handshake até o fim.
func (n *Node) handle(nc net.Conn, outgoing bool) {
	defer nc.Close()

	// Durante o handshake, cancelar n.ctx fecha o socket imediatamente.
	stopHandshakeWatch := context.AfterFunc(n.ctx, func() { nc.Close() })
	if !outgoing && !n.shares() {
		stopHandshakeWatch()
		n.logf("recusando conexão de %s: no modo cliente-servidor o cliente só conversa com a semente", nc.RemoteAddr())
		return
	}
	id, err := n.handshake(nc)
	stopHandshakeWatch()
	if err != nil {
		if n.ctx.Err() == nil {
			n.logf("handshake com %s falhou: %v", nc.RemoteAddr(), err)
		}
		return
	}

	c := newConn(n, nc, id)
	if err := n.register(c); err != nil {
		c.close()
		n.logf("recusando %s: %v", id, err)
		return
	}
	n.logf("conectado a %s (%s)", id, nc.RemoteAddr())

	// Após registrado, quem envia o FIN (CloseWrite) é a própria writeLoop ao
	// concluir o quadro corrente, evitando truncar um bloco no meio do envio.
	// Este timer garante o fechamento definitivo após closeGrace se o vizinho travar.
	var (
		graceMu    sync.Mutex
		graceTimer *time.Timer
	)
	stopShutdownWatch := context.AfterFunc(n.ctx, func() {
		graceMu.Lock()
		graceTimer = time.AfterFunc(closeGrace, func() { c.close() })
		graceMu.Unlock()
	})
	defer func() {
		stopShutdownWatch()
		graceMu.Lock()
		if graceTimer != nil {
			graceTimer.Stop()
		}
		graceMu.Unlock()
	}()

	n.wg.Add(1)
	go c.writeLoop()
	err = c.readLoop()
	n.unregister(c)
	if n.ctx.Err() == nil {
		n.logf("conexão com %s encerrada: %s", id, describe(err))
	}
}

// handshake troca infohash e id com o outro lado (formato em wire.Handshake).
// Se o outro lado compartilha outro arquivo, a conexão é recusada.
func (n *Node) handshake(nc net.Conn) (string, error) {
	nc.SetDeadline(time.Now().Add(handshakeTimeout))
	defer nc.SetDeadline(time.Time{})
	return wire.Handshake(nc, n.infoHash, n.cfg.ID)
}

func newConn(n *Node, nc net.Conn, id string) *Conn {
	np := n.m.NumPieces()
	outCap := np + 2*pipeline + 8
	if outCap > maxOutQueue {
		outCap = maxOutQueue
	}
	uploadCap := np
	if uploadCap < 2*pipeline {
		uploadCap = 2 * pipeline
	}
	if uploadCap > maxUploadQueue {
		uploadCap = maxUploadQueue
	}

	connCtx, connCancel := context.WithCancel(context.Background())
	return &Conn{
		n:       n,
		id:      id,
		nc:      nc,
		ctx:     connCtx,
		cancel:  connCancel,
		out:     make(chan outMsg, outCap),
		uploads: make(chan int, uploadCap),
		closed:  make(chan struct{}),
		has:     make([]bool, np),
	}
}

// register adiciona c aos vizinhos e já enfileira o bitfield (troca inicial:
// quais blocos temos). Um cliente do modo cliente-servidor anuncia que não
// tem nada, para que ninguém peça blocos a ele.
func (n *Node) register(c *Conn) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	switch {
	case n.ctx.Err() != nil:
		return errors.New("nó encerrando")
	case c.id == n.cfg.ID:
		return errors.New("conexão consigo mesmo (ids repetidos?)")
	case n.banned[c.id]:
		return errors.New("peer banido")
	case n.conns[c.id] != nil:
		return errors.New("já conectado")
	}
	n.conns[c.id] = c
	have := make([]bool, n.m.NumPieces())
	if n.shares() {
		have = n.store.HaveSlice()
	}
	// Enfileirado com o lock: nenhum HAVE para c sai antes do bitfield.
	c.send(wire.MsgBitfield, encodeBitfield(have))
	return nil
}

// unregister remove c dos vizinhos e libera os blocos que tínhamos pedido a
// ele, para que sejam pedidos a outro vizinho.
func (n *Node) unregister(c *Conn) {
	c.close()
	n.mu.Lock()
	delete(n.conns, c.id)
	for i, has := range c.has {
		if has {
			n.avail[i]--
		}
	}
	released := 0
	for i, req := range n.inflight {
		if req.conn == c {
			delete(n.inflight, i)
			released++
		}
	}
	n.mu.Unlock()
	if released > 0 {
		n.requestAll()
	}
}

func (c *Conn) close() {
	c.once.Do(func() {
		c.cancel()
		close(c.closed)
		c.nc.Close()
	})
}

// closeWriteGracefully envia apenas o FIN (CloseWrite) após a writeLoop
// concluir o último quadro, permitindo que o vizinho receba todos os bytes
// já colocados no socket sem receber RST.
func closeWriteGracefully(nc net.Conn) {
	if tc, ok := nc.(*net.TCPConn); ok {
		_ = tc.CloseWrite()
		return
	}
	_ = nc.Close()
}

// --- escrita ---

// send enfileira uma mensagem sem bloquear. Se a fila estiver cheia, o
// vizinho não está acompanhando e a conexão é derrubada.
func (c *Conn) send(typ uint8, payload []byte) {
	select {
	case c.out <- outMsg{typ, payload}:
	default:
		c.n.logf("fila de envio para %s cheia: derrubando a conexão", c.id)
		c.close()
	}
}

// writeLoop é a única goroutine que escreve no socket depois do handshake.
// Quando n.ctx é cancelado, encerra entre quadros (sem truncar dados) e envia
// FIN via closeWriteGracefully.
func (c *Conn) writeLoop() {
	defer c.n.wg.Done()
	keepAlive := time.NewTicker(keepAliveEvery)
	defer keepAlive.Stop()

	pieceWriter := io.Writer(c.nc)
	if c.n.upLimiter != nil {
		pieceWriter = c.n.upLimiter.LimitWriterContext(c.ctx, c.nc)
	}

	for {
		if c.n.ctx.Err() != nil {
			closeWriteGracefully(c.nc)
			return
		}
		var err error
		select {
		case <-c.closed:
			return
		case <-c.n.ctx.Done():
			closeWriteGracefully(c.nc)
			return
		case m := <-c.out:
			err = wire.WriteFrame(c.nc, m.typ, m.payload)
		case i := <-c.uploads:
			err = c.sendPiece(pieceWriter, i)
		case <-keepAlive.C:
			err = wire.WriteKeepAlive(c.nc)
		}
		if err != nil {
			if c.n.ctx.Err() == nil {
				c.close() // readLoop vai perceber a conexão fechada e registrar o motivo
			}
			return
		}
	}
}

// sendPiece lê o bloco i do disco e o envia (respeitando upLimiter, se ativo).
// Com -corrupt, estraga o bloco antes de mandar (simula um par malicioso).
func (c *Conn) sendPiece(w io.Writer, i int) error {
	n := c.n
	data, err := n.store.ReadPiece(i)
	if err != nil {
		return err
	}
	if len(data) > 0 && n.cfg.Corrupt > 0 && rand.Float64() < n.cfg.Corrupt {
		data[0] ^= 0xFF
	}
	if err := wire.WritePiece(w, uint32(i), data); err != nil {
		return err
	}
	total := n.uploaded.Add(int64(len(data)))
	if n.cfg.LeaveAfter > 0 && total >= n.cfg.LeaveAfter {
		n.leaveOnce.Do(func() {
			n.logf("já enviei %s: saindo da rede (-leave-after)", mb(total))
			n.stop()
		})
	}
	return nil
}

// --- leitura ---

// readLoop lê mensagens até a conexão cair ou o vizinho violar o protocolo.
// Se nada chegar em deadAfter (nem keep-alive), o vizinho é dado como morto.
func (c *Conn) readLoop() error {
	src := io.Reader(c.nc)
	if c.n.downLimiter != nil {
		src = c.n.downLimiter.LimitReaderContext(c.ctx, c.nc)
	}
	r := bufio.NewReaderSize(src, 64<<10)
	for {
		c.nc.SetReadDeadline(time.Now().Add(deadAfter))
		f, err := wire.ReadFrame(r)
		if err != nil {
			return err
		}
		if err := c.handleFrame(f); err != nil {
			return err
		}
	}
}

// handleFrame repassa cada mensagem para quem cuida dela. Devolver erro
// significa violação de protocolo: a conexão é encerrada.
func (c *Conn) handleFrame(f wire.Frame) error {
	n := c.n
	switch f.Type {
	case wire.MsgKeepAlive:
		return nil // só serve para mostrar que o vizinho está vivo
	case wire.MsgBitfield:
		return n.onBitfield(c, f.Payload)
	case wire.MsgHave:
		i, err := n.decodeIndex(f.Payload)
		if err != nil {
			return err
		}
		n.onHave(c, i)
		return nil
	case wire.MsgRequest:
		i, err := n.decodeIndex(f.Payload)
		if err != nil {
			return err
		}
		return c.onRequest(i)
	case wire.MsgPiece:
		idx, data, err := wire.DecodePiece(f.Payload)
		if err != nil {
			return err
		}
		if int(idx) >= n.m.NumPieces() {
			return fmt.Errorf("índice de bloco fora do intervalo: %d", idx)
		}
		return n.onPiece(c, int(idx), data)
	default:
		return fmt.Errorf("tipo de mensagem desconhecido: %d", f.Type)
	}
}

// onRequest coloca o bloco pedido na fila de envio desta conexão.
func (c *Conn) onRequest(i int) error {
	if !c.n.shares() {
		return errors.New("pediu bloco a um cliente do modo cliente-servidor, que não envia nada")
	}
	if !c.n.store.HasPiece(i) {
		return fmt.Errorf("pediu o bloco %d, que não anunciamos", i)
	}
	select {
	case c.uploads <- i: // writeLoop envia quando puder
		return nil
	default:
		return errors.New("pedidos demais em aberto")
	}
}

// decodeIndex lê o índice de bloco de um HAVE ou REQUEST e confere o intervalo.
func (n *Node) decodeIndex(payload []byte) (int, error) {
	v, err := wire.DecodeUint32(payload)
	if err != nil {
		return 0, err
	}
	if int(v) >= n.m.NumPieces() {
		return 0, fmt.Errorf("índice de bloco fora do intervalo: %d", v)
	}
	return int(v), nil
}

// describe traduz os motivos mais comuns de fim de conexão.
func describe(err error) string {
	var ne net.Error
	switch {
	case errors.Is(err, io.EOF):
		return "o vizinho fechou a conexão"
	case errors.As(err, &ne) && ne.Timeout():
		return fmt.Sprintf("nada recebido em %v (heartbeat perdido)", deadAfter)
	case errors.Is(err, net.ErrClosed), errors.Is(err, context.Canceled):
		return "conexão fechada por nós"
	default:
		return err.Error()
	}
}

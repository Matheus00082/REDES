//	picker.go    políticas de escolha de blocos (rarest-first com desempate aleatório e sequential)

package peer

import (
	"fmt"
	"math/rand"
)

// Nomes das políticas de escolha de blocos usados na configuração/CLI.
const (
	PickRarest     = "rarest"     // o bloco que está em menos vizinhos primeiro
	PickSequential = "sequential" // o bloco de menor índice primeiro
)

// Policy define a estratégia utilizada para selecionar os blocos.
type Policy int

const (
	// Sequential seleciona os blocos em ordem sequencial (menor índice primeiro).
	Sequential Policy = iota

	// RarestFirst seleciona os blocos com menor disponibilidade entre os vizinhos.
	RarestFirst
)

// String devolve o nome textual da política.
func (p Policy) String() string {
	switch p {
	case Sequential:
		return PickSequential
	case RarestFirst:
		return PickRarest
	default:
		return "unknown"
	}
}

// ParsePolicy converte o nome da política ("rarest" ou "sequential") em Policy.
func ParsePolicy(name string) (Policy, error) {
	switch name {
	case "", PickRarest:
		return RarestFirst, nil
	case PickSequential:
		return Sequential, nil
	default:
		return 0, fmt.Errorf("política desconhecida %q (use %s ou %s)", name, PickRarest, PickSequential)
	}
}

// Picker gerencia a seleção do próximo bloco para download.
type Picker struct {
	Policy   Policy
	randIntn func(int) int // opcional (para testes determinísticos); padrão: rand.Intn
}

// NewPicker cria um seletor de blocos utilizando a estratégia informada.
func NewPicker(policy Policy) *Picker {
	return &Picker{
		Policy: policy,
	}
}

// NewPickerFromName cria um seletor de blocos a partir do nome da política.
func NewPickerFromName(name string) (*Picker, error) {
	policy, err := ParsePolicy(name)
	if err != nil {
		return nil, err
	}
	return NewPicker(policy), nil
}

func (p *Picker) intn(n int) int {
	if p != nil && p.randIntn != nil {
		return p.randIntn(n)
	}
	return rand.Intn(n)
}

// Pick retorna o índice do próximo bloco elegível considerando apenas
// disponibilidade global e o estado local `have`.
// Retorna -1 quando não existe bloco elegível para download.
func (p *Picker) Pick(availability []int, have []bool) int {
	if len(availability) != len(have) {
		return -1
	}
	return p.PickMatching(availability, func(i int) bool {
		return !have[i] && availability[i] > 0
	})
}

// PickMatching retorna o índice do próximo bloco entre os que satisfazem `ok(i)`.
// É usado pelo gerenciador de download para filtrar por blocos ausentes,
// não solicitados (fora de `inflight`) e presentes em um vizinho específico.
func (p *Picker) PickMatching(availability []int, ok func(int) bool) int {
	if p == nil || ok == nil || len(availability) == 0 {
		return -1
	}

	switch p.Policy {
	case Sequential:
		return p.pickSequential(len(availability), ok)
	case RarestFirst:
		return p.pickRarestFirst(availability, ok)
	default:
		return -1
	}
}

// pickSequential devolve o menor índice i com ok(i), ou -1 se não houver.
func (p *Picker) pickSequential(n int, ok func(int) bool) int {
	for i := 0; i < n; i++ {
		if ok(i) {
			return i
		}
	}
	return -1
}

// pickRarestFirst devolve, entre os índices com ok(i), um dos que estão em
// menos vizinhos (menor availability[i]), sorteando uniformemente entre os
// empatados (reservoir sampling).
//
// O sorteio importa: no começo todos os blocos só existem na semente
// (disponibilidade 1) e, sem ele, todos os peers pediriam os mesmos blocos.
func (p *Picker) pickRarestFirst(availability []int, ok func(int) bool) int {
	best, ties := -1, 0
	for i, a := range availability {
		if !ok(i) {
			continue
		}
		switch {
		case best < 0 || a < availability[best]:
			best, ties = i, 1
		case a == availability[best]:
			ties++
			if p.intn(ties) == 0 { // cada empatado tem a mesma chance
				best = i
			}
		}
	}
	return best
}

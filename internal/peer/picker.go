package peer

// Policy define a estratégia utilizada para selecionar os blocos.
type Policy int

const (
	// Sequential seleciona os blocos em ordem sequencial.
	Sequential Policy = iota

	// RarestFirst seleciona os blocos com menor disponibilidade.
	RarestFirst
)

// Picker gerencia a seleção do próximo bloco para download.
type Picker struct {
	Policy Policy
}

// NewPicker cria um seletor de blocos utilizando a estratégia informada.
func NewPicker(policy Policy) *Picker {
	return &Picker{
		Policy: policy,
	}
}

// Pick retorna o índice do próximo bloco a ser baixado.
//
// availability[i] representa o número de peers ativos que possuem o bloco i.
// have[i] indica se o peer local já possui o bloco i.
//
// Retorna -1 quando não existe bloco elegível para download.
func (p *Picker) Pick(
	availability []int,
	have []bool,
) int {
	if p == nil {
		return -1
	}

	if len(availability) != len(have) {
		return -1
	}

	switch p.Policy {
	case Sequential:
		return p.pickSequential(availability, have)

	case RarestFirst:
		return p.pickRarestFirst(availability, have)

	default:
		return -1
	}
}

// pickSequential seleciona o primeiro bloco que ainda não foi baixado
// e que está disponível em pelo menos um peer ativo.
func (p *Picker) pickSequential(
	availability []int,
	have []bool,
) int {
	for i := range availability {
		if !have[i] && availability[i] > 0 {
			return i
		}
	}

	return -1
}

// pickRarestFirst seleciona o bloco ainda não baixado
// com menor disponibilidade entre os peers ativos.
//
// Em caso de empate, seleciona o bloco com menor índice.
func (p *Picker) pickRarestFirst(
	availability []int,
	have []bool,
) int {
	selected := -1
	minAvailability := int(^uint(0) >> 1)

	for i, count := range availability {
		if have[i] || count <= 0 {
			continue
		}

		if count < minAvailability {
			minAvailability = count
			selected = i
		}
	}

	return selected
}

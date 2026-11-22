package peer

import (
	"crypto/sha256"
	"fmt"
)

// swarmPeer representa um peer em uma simulação em memória do enxame.
type swarmPeer struct {
	id          string
	blocks      map[int][]byte
	uploadCount int
}

func newSwarmPeer(id string) *swarmPeer {
	return &swarmPeer{
		id:     id,
		blocks: make(map[int][]byte),
	}
}

func (p *swarmPeer) addBlock(index int, data []byte) {
	p.blocks[index] = append([]byte(nil), data...)
}

func (p *swarmPeer) hasBlock(index int) bool {
	_, ok := p.blocks[index]
	return ok
}

// swarmAvailability conta quantos peers possuem cada bloco.
func swarmAvailability(peers []*swarmPeer, totalBlocks int) []int {
	if totalBlocks <= 0 {
		return nil
	}

	availability := make([]int, totalBlocks)
	for _, p := range peers {
		if p == nil {
			continue
		}
		for index := range p.blocks {
			if index >= 0 && index < totalBlocks {
				availability[index]++
			}
		}
	}
	return availability
}

func findBlockSource(peers []*swarmPeer, requester *swarmPeer, blockIndex int) *swarmPeer {
	for _, p := range peers {
		if p == nil || requester == nil || p.id == requester.id {
			continue
		}
		if p.hasBlock(blockIndex) {
			return p
		}
	}
	return nil
}

type swarmTransfer struct {
	source      *swarmPeer
	destination *swarmPeer
	blockIndex  int
}

// planSwarmRound planeja uma rodada usando um único snapshot de disponibilidade.
// Blocos selecionados são reservados para evitar pedidos duplicados ao mesmo destino.
func planSwarmRound(
	peers []*swarmPeer,
	picker *Picker,
	totalBlocks int,
	maxParallel int,
) []swarmTransfer {
	if totalBlocks <= 0 || maxParallel <= 0 || picker == nil {
		return nil
	}

	availability := swarmAvailability(peers, totalBlocks)
	transfers := make([]swarmTransfer, 0)
	for _, destination := range peers {
		if destination == nil {
			continue
		}
		have := make([]bool, totalBlocks)
		for blockIndex := range destination.blocks {
			if blockIndex >= 0 && blockIndex < totalBlocks {
				have[blockIndex] = true
			}
		}

		for request := 0; request < maxParallel; request++ {
			blockIndex := picker.Pick(availability, have)
			if blockIndex < 0 {
				break
			}
			if blockIndex >= totalBlocks {
				break
			}
			have[blockIndex] = true

			source := findBlockSource(peers, destination, blockIndex)
			if source == nil {
				continue
			}
			transfers = append(transfers, swarmTransfer{
				source:      source,
				destination: destination,
				blockIndex:  blockIndex,
			})
		}
	}
	return transfers
}

// transferBlock valida o hash do bloco antes de copiá-lo ao destino.
func transferBlock(
	source *swarmPeer,
	destination *swarmPeer,
	blockIndex int,
	expectedHashes [][32]byte,
) error {
	if source == nil {
		return fmt.Errorf("source peer is nil")
	}
	if destination == nil {
		return fmt.Errorf("destination peer is nil")
	}
	if blockIndex < 0 || blockIndex >= len(expectedHashes) {
		return fmt.Errorf("block index %d is outside the expected hashes", blockIndex)
	}

	data, ok := source.blocks[blockIndex]
	if !ok {
		return fmt.Errorf("peer %s does not have block %d", source.id, blockIndex)
	}
	if hash := sha256.Sum256(data); hash != expectedHashes[blockIndex] {
		return fmt.Errorf("block %d failed SHA-256 validation", blockIndex)
	}

	destination.addBlock(blockIndex, data)
	source.uploadCount++
	return nil
}

// swarmCompleted verifica se todos os peers possuem todos os blocos.
func swarmCompleted(peers []*swarmPeer, totalBlocks int) bool {
	if totalBlocks <= 0 || len(peers) == 0 {
		return false
	}
	for _, p := range peers {
		if p == nil || len(p.blocks) != totalBlocks {
			return false
		}
		for i := 0; i < totalBlocks; i++ {
			if !p.hasBlock(i) {
				return false
			}
		}
	}
	return true
}

// reconstructFile remonta o arquivo concatenando os blocos em ordem.
func reconstructFile(p *swarmPeer, totalBlocks int) ([]byte, error) {
	if p == nil {
		return nil, fmt.Errorf("peer is nil")
	}
	if totalBlocks < 0 {
		return nil, fmt.Errorf("total block count cannot be negative")
	}

	result := make([]byte, 0)
	for index := 0; index < totalBlocks; index++ {
		data, ok := p.blocks[index]
		if !ok {
			return nil, fmt.Errorf("peer %s is missing block %d", p.id, index)
		}
		result = append(result, data...)
	}
	return result, nil
}

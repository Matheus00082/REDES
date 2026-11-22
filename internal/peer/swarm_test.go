package peer

import (
	"crypto/sha256"
	"testing"
)

// runSwarm simulates parallel block propagation after the original seed leaves.
//
// All requests for a round are planned before any transfer is applied, so
// every peer selects from the same availability snapshot.
// The simulation stops when all peers have all blocks or
// no further progress can be made.
func runSwarm(
	t *testing.T,
	policy Policy,
	peers []*swarmPeer,
	expectedHashes [][32]byte,
	totalBlocks int,
) {
	t.Helper()

	picker := NewPicker(policy)

	maxRounds := totalBlocks * len(peers) * 2

	for round := 0; round < maxRounds; round++ {
		transfers := planSwarmRound(peers, picker, totalBlocks, 2)
		if len(transfers) == 0 {
			break
		}

		for _, transfer := range transfers {
			if err := transferBlock(
				transfer.source,
				transfer.destination,
				transfer.blockIndex,
				expectedHashes,
			); err != nil {
				t.Fatalf("transfer failed: %v", err)
			}
		}

		if swarmCompleted(peers, totalBlocks) {
			return
		}
	}

	if !swarmCompleted(peers, totalBlocks) {
		t.Fatalf("swarm failed to distribute all blocks")
	}
}

// TestSwarmBlockExchange verifies that peers can exchange blocks
// and reconstruct the complete file after the original seed leaves.
func TestSwarmBlockExchange(t *testing.T) {
	originalData := []byte(
		"Block-0|Block-1|Block-2|Block-3|Block-4|Block-5",
	)

	blocks := [][]byte{
		[]byte("Block-0|"),
		[]byte("Block-1|"),
		[]byte("Block-2|"),
		[]byte("Block-3|"),
		[]byte("Block-4|"),
		[]byte("Block-5"),
	}

	totalBlocks := len(blocks)

	var expectedHashes [][32]byte

	for _, block := range blocks {
		expectedHashes = append(
			expectedHashes,
			sha256.Sum256(block),
		)
	}

	for _, test := range []struct {
		name   string
		policy Policy
	}{
		{name: "sequential", policy: Sequential},
		{name: "rarest-first", policy: RarestFirst},
	} {
		t.Run(test.name, func(t *testing.T) {
			// Simulate peers remaining after the original seed leaves.
			peer1 := newSwarmPeer("peer-1")
			peer2 := newSwarmPeer("peer-2")
			peer3 := newSwarmPeer("peer-3")
			peer4 := newSwarmPeer("peer-4")

			peer1.addBlock(0, blocks[0])
			peer1.addBlock(1, blocks[1])
			peer2.addBlock(2, blocks[2])
			peer2.addBlock(3, blocks[3])
			peer3.addBlock(4, blocks[4])
			peer3.addBlock(5, blocks[5])

			peers := []*swarmPeer{peer1, peer2, peer3, peer4}
			runSwarm(t, test.policy, peers, expectedHashes, totalBlocks)

			for _, p := range peers {
				if len(p.blocks) != totalBlocks {
					t.Errorf("%s has %d blocks, want %d", p.id, len(p.blocks), totalBlocks)
				}

				got, err := reconstructFile(p, totalBlocks)
				if err != nil {
					t.Fatalf("reconstruction failed for %s: %v", p.id, err)
				}
				if string(got) != string(originalData) {
					t.Errorf("%s reconstructed %q; want %q", p.id, got, originalData)
				}
			}

			totalUploads := peer1.uploadCount +
				peer2.uploadCount +
				peer3.uploadCount +
				peer4.uploadCount
			if totalUploads == 0 {
				t.Fatal("expected peers to upload blocks to each other")
			}
		})
	}
}

func TestRarestFirstReplicatesRareBlocksFirstAfterSeedLeaves(t *testing.T) {
	availabilityAfterRound := func(policy Policy) []int {
		peer1 := newSwarmPeer("peer-1")
		peer2 := newSwarmPeer("peer-2")
		peer3 := newSwarmPeer("peer-3")
		peer4 := newSwarmPeer("peer-4")

		// The original seed is absent. Block 2 has one remaining copy,
		// while block 1 has two copies.
		peer1.addBlock(0, []byte("0"))
		peer1.addBlock(1, []byte("1"))
		peer1.addBlock(2, []byte("2"))
		peer2.addBlock(0, []byte("0"))
		peer3.addBlock(0, []byte("0"))
		peer4.addBlock(0, []byte("0"))
		peer4.addBlock(1, []byte("1"))

		peers := []*swarmPeer{peer1, peer2, peer3, peer4}
		transfers := planSwarmRound(peers, NewPicker(policy), 3, 1)
		if len(transfers) != 3 {
			t.Fatalf("%v planned %d transfers; want 3", policy, len(transfers))
		}

		expectedHashes := [][32]byte{
			sha256.Sum256([]byte("0")),
			sha256.Sum256([]byte("1")),
			sha256.Sum256([]byte("2")),
		}
		for _, transfer := range transfers {
			if err := transferBlock(
				transfer.source,
				transfer.destination,
				transfer.blockIndex,
				expectedHashes,
			); err != nil {
				t.Fatalf("transfer failed: %v", err)
			}
		}

		return swarmAvailability(peers, 3)
	}

	sequential := availabilityAfterRound(Sequential)
	rarest := availabilityAfterRound(RarestFirst)
	if rarest[2] <= sequential[2] {
		t.Fatalf(
			"rarest-first replicated block 2 %d times; sequential replicated it %d times",
			rarest[2],
			sequential[2],
		)
	}
	if rarest[2] != 4 || sequential[2] != 2 {
		t.Fatalf(
			"unexpected block-2 availability: rarest-first=%d, sequential=%d; want 4 and 2",
			rarest[2],
			sequential[2],
		)
	}
}

func TestParallelSwarmRequestsReserveDistinctBlocks(t *testing.T) {
	source := newSwarmPeer("source")
	destination := newSwarmPeer("destination")
	source.addBlock(0, []byte("0"))
	source.addBlock(1, []byte("1"))
	source.addBlock(2, []byte("2"))

	transfers := planSwarmRound(
		[]*swarmPeer{source, destination},
		NewPicker(Sequential),
		3,
		3,
	)
	if len(transfers) != 3 {
		t.Fatalf("planned %d transfers; want 3", len(transfers))
	}

	selected := make(map[int]bool)
	for _, transfer := range transfers {
		if selected[transfer.blockIndex] {
			t.Fatalf("block %d was requested more than once", transfer.blockIndex)
		}
		selected[transfer.blockIndex] = true
	}
}

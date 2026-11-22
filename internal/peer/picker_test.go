package peer

import "testing"

func TestPickerSequential(t *testing.T) {
	picker := NewPicker(Sequential)

	availability := []int{2, 3, 1, 4, 2}
	have := []bool{false, false, false, false, false}

	got := picker.Pick(availability, have)

	if got != 0 {
		t.Fatalf("Sequential: esperava bloco 0, obteve %d", got)
	}
}

func TestPickerSequentialSkipsUnavailableAndCompleted(t *testing.T) {
	picker := NewPicker(Sequential)

	availability := []int{0, 2, 0, 1, 3}
	have := []bool{false, false, true, false, false}

	got := picker.Pick(availability, have)

	if got != 1 {
		t.Fatalf("esperava bloco 1, obteve %d", got)
	}
}

func TestPickerRarestFirst(t *testing.T) {
	picker := NewPicker(RarestFirst)

	availability := []int{4, 2, 5, 1, 3}
	have := []bool{false, false, false, false, false}

	got := picker.Pick(availability, have)

	if got != 3 {
		t.Fatalf("RarestFirst: esperava bloco 3, obteve %d", got)
	}
}

func TestPickerRarestFirstSkipsCompleted(t *testing.T) {
	picker := NewPicker(RarestFirst)

	availability := []int{1, 2, 1, 3, 4}
	have := []bool{true, false, false, false, false}

	got := picker.Pick(availability, have)

	if got != 2 {
		t.Fatalf("esperava bloco 2, obteve %d", got)
	}
}

func TestPickerRarestFirstTie(t *testing.T) {
	picker := NewPicker(RarestFirst)

	availability := []int{2, 1, 1, 3}
	have := []bool{false, false, false, false}

	got := picker.Pick(availability, have)

	// Em caso de empate, escolhe o menor índice.
	if got != 1 {
		t.Fatalf("esperava bloco 1 no desempate, obteve %d", got)
	}
}

func TestPickerNoAvailableBlocks(t *testing.T) {
	picker := NewPicker(RarestFirst)

	availability := []int{0, 0, 0, 0}
	have := []bool{false, false, false, false}

	got := picker.Pick(availability, have)

	if got != -1 {
		t.Fatalf("esperava -1 sem blocos disponíveis, obteve %d", got)
	}
}

func TestPickerAllBlocksCompleted(t *testing.T) {
	picker := NewPicker(Sequential)

	availability := []int{2, 3, 1}
	have := []bool{true, true, true}

	got := picker.Pick(availability, have)

	if got != -1 {
		t.Fatalf("esperava -1 com todos os blocos completos, obteve %d", got)
	}
}

func TestPickerMismatchedSlices(t *testing.T) {
	picker := NewPicker(Sequential)

	availability := []int{2, 1, 3}
	have := []bool{false, false}

	got := picker.Pick(availability, have)

	if got != -1 {
		t.Fatalf("esperava -1 para slices incompatíveis, obteve %d", got)
	}
}

func TestPickerInvalidPolicy(t *testing.T) {
	picker := NewPicker(Policy(99))

	availability := []int{2, 1, 3}
	have := []bool{false, false, false}

	got := picker.Pick(availability, have)

	if got != -1 {
		t.Fatalf("esperava -1 para policy inválida, obteve %d", got)
	}
}

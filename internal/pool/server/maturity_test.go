package server

import "testing"

func TestMaxEligibleBlockHeight(t *testing.T) {
	tests := []struct {
		name                  string
		currentHeight         uint64
		requiredConfirmations int
		wantHeight            uint64
		wantOK                bool
	}{
		{
			name:                  "depth one uses current height",
			currentHeight:         100,
			requiredConfirmations: 1,
			wantHeight:            100,
			wantOK:                true,
		},
		{
			name:                  "depth zero preserves legacy immediate eligibility",
			currentHeight:         100,
			requiredConfirmations: 0,
			wantHeight:            100,
			wantOK:                true,
		},
		{
			name:                  "normal depth returns oldest eligible cutoff",
			currentHeight:         100,
			requiredConfirmations: 10,
			wantHeight:            91,
			wantOK:                true,
		},
		{
			name:                  "not enough chain height avoids underflow",
			currentHeight:         8,
			requiredConfirmations: 10,
			wantOK:                false,
		},
		{
			name:                  "genesis can satisfy depth one",
			currentHeight:         0,
			requiredConfirmations: 1,
			wantHeight:            0,
			wantOK:                true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotHeight, gotOK := maxEligibleBlockHeight(tt.currentHeight, tt.requiredConfirmations)
			if gotOK != tt.wantOK {
				t.Fatalf("ok = %v, want %v", gotOK, tt.wantOK)
			}
			if gotHeight != tt.wantHeight {
				t.Fatalf("height = %d, want %d", gotHeight, tt.wantHeight)
			}
		})
	}
}

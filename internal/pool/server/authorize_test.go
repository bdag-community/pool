package server

import "testing"

func TestValidateUserEVMAddress(t *testing.T) {
	tests := []struct {
		name       string
		username   string
		wantValid  bool
		wantReason string
	}{
		{name: "valid lower", username: "0x05518e03e148c56e426ff9e1cbdb962b4fc5250a", wantValid: true},
		{name: "valid mixed", username: "0x05518E03e148C56e426ff9e1CBdB962B4FC5250A", wantValid: true},
		{name: "empty", username: "", wantReason: "empty username"},
		{name: "missing prefix", username: "05518E03e148C56e426ff9e1CBdB962B4FC5250A", wantReason: "username should be a valid evm address (start with 0x...)"},
		{name: "invalid hex", username: "0x05518E03e148C56e426ff9e1CBdB962B4FC5250Z", wantReason: "username should be a valid evm address"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotValid, gotReason := validateUser(tt.username)
			if gotValid != tt.wantValid {
				t.Fatalf("valid = %v, want %v", gotValid, tt.wantValid)
			}
			if gotReason != tt.wantReason {
				t.Fatalf("reason = %q, want %q", gotReason, tt.wantReason)
			}
		})
	}
}

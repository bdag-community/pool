package main

import "testing"

func TestApprovedRPCPairsRemainExactAndCannotMix(t *testing.T) {
	for _, pair := range [][2]string{{"http://127.0.0.1:48132", "http://127.0.0.1:18547"}, {"http://127.0.0.1:48142", "http://127.0.0.1:18557"}} {
		a, _ := approvedFixture(t)
		a.NativeURL, a.EVMURL = pair[0], pair[1]
		path, digest := approvalFile(t, a)
		if _, err := readApproval(path, digest); err != nil {
			t.Fatalf("exact owned sibling pair rejected: %v", err)
		}
	}
	for _, pair := range [][2]string{
		{"http://127.0.0.1:48132", "http://127.0.0.1:18557"},
		{"http://127.0.0.1:48142", "http://127.0.0.1:18547"},
		{"http://192.168.100.16:48142", "http://127.0.0.1:18557"},
		{"http://localhost:48142", "http://127.0.0.1:18557"},
		{"http://127.0.0.1:48142/", "http://127.0.0.1:18557"},
		{"http://user:pass@127.0.0.1:48142", "http://127.0.0.1:18557"},
	} {
		a, _ := approvedFixture(t)
		a.NativeURL, a.EVMURL = pair[0], pair[1]
		path, digest := approvalFile(t, a)
		if _, err := readApproval(path, digest); err == nil {
			t.Fatal("unapproved or mixed RPC pair accepted")
		}
	}
}

package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAdminRequestExactWeiAndStrictMetadata(t *testing.T) {
	path := filepath.Join(t.TempDir(), "request.json")
	body := `{"Wallet":{"ChainID":1404,"Sender":"0x2222222222222222222222222222222222222222"},"Missing":{"ID":39239,"Amount":232504200630000000000}}`
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	q, err := readRequest(path)
	if err != nil || q.Missing.Amount.String() != "232504200630000000000" || q.Missing.ID != 39239 || q.Wallet.ChainID != 1404 {
		t.Fatal("request lost exact money identity", err)
	}
	for _, bad := range []string{body + " {}", `{"ForceOverride":true}`, "not-json"} {
		if err := os.WriteFile(path, []byte(bad), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := readRequest(path); err == nil {
			t.Fatal("invalid/override input accepted")
		}
	}
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := readRequest(path); err == nil {
		t.Fatal("public request file accepted")
	}
}

func TestAdminFailureClassificationCannotExposeSecrets(t *testing.T) {
	secret := "private-connection-password-must-not-appear"
	code := failureCode(errors.New("connection failed: " + secret))
	if strings.Contains(code, secret) || code != "unclassified_rejection" {
		t.Fatal("unsafe diagnostic code")
	}
	if failureCode(errors.New("canonical nonce replacement body mismatch")) != "replacement_body" {
		t.Fatal("missing focused classification")
	}
}

func TestAdminCannotReceivePrivateKeyOrDefaultToUnknownDatabase(t *testing.T) {
	t.Setenv("POOL_PRIVATE_KEY", "public-fixture-not-a-real-key")
	if err := run([]string{"schema-check"}); err == nil {
		t.Fatal("admin accepted a signing key")
	}
	t.Setenv("POOL_PRIVATE_KEY", "")
	t.Setenv("PG_URL", "")
	if err := run([]string{"schema-check"}); err == nil {
		t.Fatal("admin used implicit database")
	}
	if err := run([]string{"unknown"}); err == nil {
		t.Fatal("unknown operation accepted")
	}
}

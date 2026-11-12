package storage

import (
	"context"
	"errors"
	"math/big"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestBuildAddCreditsInsert(t *testing.T) {
	query, args := buildAddCreditsInsert("block-1", map[string]uint64{
		"miner-c": 30,
		"miner-a": 10,
		"miner-b": 20,
	})

	wantQuery := "INSERT INTO credits (block_hash, miner_address, amount) VALUES ($1,$2,$3),($1,$4,$5),($1,$6,$7)"
	if query != wantQuery {
		t.Fatalf("query mismatch\nwant: %s\n got: %s", wantQuery, query)
	}

	wantArgs := []any{
		"block-1",
		"miner-a", uint64(10),
		"miner-b", uint64(20),
		"miner-c", uint64(30),
	}
	if len(args) != len(wantArgs) {
		t.Fatalf("arg count mismatch: want %d got %d", len(wantArgs), len(args))
	}
	for i := range wantArgs {
		if args[i] != wantArgs[i] {
			t.Fatalf("arg %d mismatch: want %#v got %#v", i, wantArgs[i], args[i])
		}
	}
}

func TestBuildRecordBlockSubmissionInsert(t *testing.T) {
	query, args := buildRecordBlockSubmissionInsert(BlockSubmission{
		CandidateHash: "candidate-1",
		NodeBlockHash: "node-1",
		Height:        123,
		BackendName:   "node",
		TemplateSeq:   77,
		Accepted:      true,
		Outcome:       "ok",
		Message:       "accepted",
	})

	wantQuery := "INSERT INTO block_submissions (candidate_hash, node_block_hash, height, backend, template_seq, accepted, outcome, message) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)"
	if query != wantQuery {
		t.Fatalf("query mismatch\nwant: %s\n got: %s", wantQuery, query)
	}

	wantArgs := []any{"candidate-1", "node-1", int64(123), "node", int64(77), true, "ok", "accepted"}
	if len(args) != len(wantArgs) {
		t.Fatalf("arg count mismatch: want %d got %d", len(wantArgs), len(args))
	}
	for i := range wantArgs {
		if args[i] != wantArgs[i] {
			t.Fatalf("arg %d mismatch: want %#v got %#v", i, wantArgs[i], args[i])
		}
	}
}

func BenchmarkBuildAddCreditsInsert8Miners(b *testing.B) {
	credits := map[string]uint64{
		"miner-1": 1,
		"miner-2": 2,
		"miner-3": 3,
		"miner-4": 4,
		"miner-5": 5,
		"miner-6": 6,
		"miner-7": 7,
		"miner-8": 8,
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		query, args := buildAddCreditsInsert("block-1", credits)
		if len(query) == 0 || len(args) != 1+len(credits)*2 {
			b.Fatal("invalid insert payload")
		}
	}
}

func TestScanBlocksReturnsRowsErr(t *testing.T) {
	wantErr := errors.New("late rows failure")

	blocks, err := scanBlocks(&fakeRows{err: wantErr})
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected rows error %v, got %v", wantErr, err)
	}
	if blocks != nil {
		t.Fatalf("expected nil blocks on rows error, got %#v", blocks)
	}
}

func TestScanCreditsReturnsRowsErr(t *testing.T) {
	wantErr := errors.New("late credit rows failure")

	credits, err := scanCredits(&fakeRows{err: wantErr})
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected rows error %v, got %v", wantErr, err)
	}
	if credits != nil {
		t.Fatalf("expected nil credits on rows error, got %#v", credits)
	}
}

func TestMarkCreditPaidInTxExecutesCreditAndPayout(t *testing.T) {
	tx := &fakeCreditPaidExecer{}

	err := markCreditPaidInTx(context.Background(), tx, 42, "0xtx", big.NewInt(123), "0xminer")
	if err != nil {
		t.Fatalf("markCreditPaidInTx returned error: %v", err)
	}
	if len(tx.calls) != 2 {
		t.Fatalf("Exec calls = %d, want 2", len(tx.calls))
	}
	if !strings.Contains(tx.calls[0].query, "UPDATE credits SET is_paid = TRUE") {
		t.Fatalf("first query = %q, want credit update", tx.calls[0].query)
	}
	if got := tx.calls[0].args; len(got) != 1 || got[0] != 42 {
		t.Fatalf("credit update args = %#v, want credit id 42", got)
	}
	if !strings.Contains(tx.calls[1].query, "INSERT INTO payouts") {
		t.Fatalf("second query = %q, want payout insert", tx.calls[1].query)
	}
	if got := tx.calls[1].args; len(got) != 2 || got[0] != "0xtx" || got[1] != "123" {
		t.Fatalf("payout insert args = %#v, want tx hash and wei string", got)
	}
}

func TestMarkCreditPaidInTxReturnsPayoutInsertError(t *testing.T) {
	wantErr := errors.New("insert failed")
	tx := &fakeCreditPaidExecer{errAt: 2, err: wantErr}

	err := markCreditPaidInTx(context.Background(), tx, 42, "0xtx", big.NewInt(123), "0xminer")
	if !errors.Is(err, wantErr) {
		t.Fatalf("markCreditPaidInTx error = %v, want %v", err, wantErr)
	}
	if len(tx.calls) != 2 {
		t.Fatalf("Exec calls = %d, want update then failed insert", len(tx.calls))
	}
}

type fakeRows struct {
	err error
}

func (r *fakeRows) Close() {}

func (r *fakeRows) Err() error {
	return r.err
}

func (r *fakeRows) CommandTag() pgconn.CommandTag {
	return pgconn.CommandTag{}
}

func (r *fakeRows) FieldDescriptions() []pgconn.FieldDescription {
	return nil
}

func (r *fakeRows) Next() bool {
	return false
}

func (r *fakeRows) Scan(dest ...any) error {
	return errors.New("unexpected scan")
}

func (r *fakeRows) Values() ([]any, error) {
	return nil, nil
}

func (r *fakeRows) RawValues() [][]byte {
	return nil
}

func (r *fakeRows) Conn() *pgx.Conn {
	return nil
}

type fakeCreditPaidExecer struct {
	calls []fakeCreditPaidCall
	errAt int
	err   error
}

type fakeCreditPaidCall struct {
	query string
	args  []any
}

func (e *fakeCreditPaidExecer) Exec(ctx context.Context, query string, args ...any) (pgconn.CommandTag, error) {
	e.calls = append(e.calls, fakeCreditPaidCall{query: query, args: append([]any(nil), args...)})
	if e.errAt > 0 && len(e.calls) == e.errAt {
		return pgconn.CommandTag{}, e.err
	}
	return pgconn.CommandTag{}, nil
}

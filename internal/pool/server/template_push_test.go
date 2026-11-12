package server

import "testing"

// The node emits a template on every rebuild, far faster than a template can be
// broadcast to the miners. The reader must never block, and must never hand the
// consumer anything but the newest push — otherwise the socket backlogs and the
// miners drift further behind the chain tip every second.
func TestQueueLatestTemplatePushKeepsOnlyNewest(t *testing.T) {
	slot := make(chan *remoteGBTResult, 1)

	for seq := uint64(1); seq <= 100; seq++ {
		queueLatestTemplatePush(slot, &remoteGBTResult{Seq: seq})
	}

	if len(slot) != 1 {
		t.Fatalf("slot holds %d templates, want 1", len(slot))
	}
	got := <-slot
	if got.Seq != 100 {
		t.Fatalf("consumer got seq %d, want the newest push (100)", got.Seq)
	}
}

func TestQueueLatestTemplatePushDoesNotBlockOnFullSlot(t *testing.T) {
	slot := make(chan *remoteGBTResult, 1)
	queueLatestTemplatePush(slot, &remoteGBTResult{Seq: 1})

	done := make(chan struct{})
	go func() {
		defer close(done)
		queueLatestTemplatePush(slot, &remoteGBTResult{Seq: 2})
	}()

	<-done // would deadlock if the reader blocked on a full slot
	if got := <-slot; got.Seq != 2 {
		t.Fatalf("slot holds seq %d, want 2", got.Seq)
	}
}

package commitguard

import (
	"context"
	"testing"
)

func TestBarrierKeepsProducerLockedThroughCommit(t *testing.T) {
	locked := false
	committed := false
	ctx := With(context.Background(), func() (func(), func(), error) {
		locked = true
		return func() {
			if !locked {
				t.Fatal("commit ran after release")
			}
			committed = true
		}, func() { locked = false }, nil
	})
	commit, release, err := Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	commit()
	release()
	if locked || !committed {
		t.Fatalf("barrier state: locked=%t committed=%t", locked, committed)
	}
}

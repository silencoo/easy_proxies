// Package commitguard carries an optional linearization barrier through a
// context. It lets a configuration producer hold its generation lock across
// the runtime's final adopt operation without coupling the runtime manager to
// that producer's implementation.
package commitguard

import (
	"context"
	"errors"
)

type contextKey struct{}

// AcquireFunc acquires the producer's generation lock. commit is invoked only
// after the runtime configuration has been published; release must always be
// called and drops the producer lock.
type AcquireFunc func() (commit func(), release func(), err error)

// With attaches a final-publish barrier to ctx.
func With(ctx context.Context, acquire AcquireFunc) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if acquire == nil {
		return ctx
	}
	return context.WithValue(ctx, contextKey{}, acquire)
}

// Acquire obtains the optional barrier. Missing callbacks are normalized to
// no-ops so callers can use one unconditional commit/release path.
func Acquire(ctx context.Context) (commit func(), release func(), err error) {
	noop := func() {}
	if ctx == nil {
		return noop, noop, nil
	}
	acquire, _ := ctx.Value(contextKey{}).(AcquireFunc)
	if acquire == nil {
		return noop, noop, nil
	}
	commit, release, err = acquire()
	if err != nil {
		if release != nil {
			release()
		}
		return nil, nil, err
	}
	if release == nil {
		return nil, nil, errors.New("commit guard returned a nil release callback")
	}
	if commit == nil {
		commit = noop
	}
	return commit, release, nil
}

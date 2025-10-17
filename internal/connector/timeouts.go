package connector

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-logr/logr"
)

type hangWatcher struct {
	timeout time.Duration
	cancel  context.CancelFunc
	log     logr.Logger
	resetCh chan struct{}
	stopCh  chan struct{}
	once    sync.Once
}

func newHangWatcher(timeout time.Duration, cancel context.CancelFunc, log logr.Logger) *hangWatcher {
	if timeout <= 0 || cancel == nil {
		return nil
	}
	hw := &hangWatcher{
		timeout: timeout,
		cancel:  cancel,
		log:     log,
		resetCh: make(chan struct{}, 1),
		stopCh:  make(chan struct{}),
	}
	go hw.loop()
	return hw
}

func (w *hangWatcher) loop() {
	timer := time.NewTimer(w.timeout)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			if w.log.GetSink() != nil {
				w.log.Error(fmt.Errorf("hang timeout"), "Transport hang timeout triggered", "timeout", w.timeout)
			}
			w.cancel()
			return
		case <-w.resetCh:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(w.timeout)
		case <-w.stopCh:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return
		}
	}
}

func (w *hangWatcher) Touch() {
	if w == nil {
		return
	}
	select {
	case <-w.stopCh:
	case w.resetCh <- struct{}{}:
	default:
	}
}

func (w *hangWatcher) Stop() {
	if w == nil {
		return
	}
	w.once.Do(func() {
		close(w.stopCh)
	})
}

func callWithTimeout(
	ctx context.Context,
	timeout time.Duration,
	cancel context.CancelFunc,
	opName string,
	fn func() error,
) error {
	if timeout <= 0 {
		return fn()
	}
	errCh := make(chan error, 1)
	go func() {
		errCh <- fn()
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		return err
	case <-timer.C:
		if cancel != nil {
			cancel()
		}
		return fmt.Errorf("%s timed out after %s", opName, timeout)
	}
}

type asyncCallResult[T any] struct {
	value T
	err   error
}

func recvWithTimeout[T any](
	ctx context.Context,
	timeout time.Duration,
	cancel context.CancelFunc,
	opName string,
	fn func() (T, error),
) (T, error) {
	var zero T
	if timeout <= 0 {
		return fn()
	}
	resultCh := make(chan asyncCallResult[T], 1)
	go func() {
		val, err := fn()
		resultCh <- asyncCallResult[T]{value: val, err: err}
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return zero, ctx.Err()
	case res := <-resultCh:
		return res.value, res.err
	case <-timer.C:
		if cancel != nil {
			cancel()
		}
		return zero, fmt.Errorf("%s timed out after %s", opName, timeout)
	}
}

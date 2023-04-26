package throttler

import "time"

type Noop struct {
	currentLag   time.Duration // used for testing
	lagTolerance time.Duration // used for testing
}

var _ Throttler = &Noop{}

func (t *Noop) Open() error {
	return nil
}

func (t *Noop) Close() error {
	return nil
}

func (t *Noop) IsThrottled() bool {
	return t.currentLag > t.lagTolerance
}

func (t *Noop) BlockWait() {
}

func (t *Noop) UpdateLag() error {
	return nil
}

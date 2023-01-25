package throttler

type Noop struct{}

var _ Throttler = &Noop{}

func (t *Noop) Start() error {
	return nil
}

func (t *Noop) IsThrottled() bool {
	return false
}

func (t *Noop) BlockWait() {
}

func (t *Noop) UpdateLag() error {
	return nil
}

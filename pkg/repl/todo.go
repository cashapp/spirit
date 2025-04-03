package repl

func (c *Client) SetTableChangeNotification(newChan chan string) {
	c.Lock()
	defer c.Unlock()

	c.onDDL = newChan
}

// KeyAboveWatermarkEnabled returns true if the key above watermark optimization is enabled.
// and it's also safe to do so.
func (c *Client) KeyAboveWatermarkEnabled() bool {
	c.Lock()
	defer c.Unlock()
	//return c.enableKeyAboveWatermark && c.KeyAboveCopierCallback != nil
	return false
}

func (c *Client) SetKeyAboveWatermarkOptimization(newVal bool) {
	c.Lock()
	defer c.Unlock()

	// c.enableKeyAboveWatermark = newVal
}

func (c *Client) SetKeyAboveCopierCallback(newFunc func(interface{}) bool) {
	c.Lock()
	defer c.Unlock()

	// c.KeyAboveCopierCallback = newFunc
}

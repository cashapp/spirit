package repl

import (
	"fmt"

	"github.com/siddontang/loggers"
)

// Canal accepts a loggers.advanced. If we pass our loggers.Advanced directly,
// it will write a bunch of spam to the log. So we use this hack
// to filter out the noisiest messages.

func NewLogWrapper(logger loggers.Advanced) *LogWrapper {
	return &LogWrapper{
		logger: logger,
	}
}

var _ loggers.Advanced = &LogWrapper{}

type LogWrapper struct {
	logger loggers.Advanced
}

func (c *LogWrapper) Debugf(format string, args ...interface{}) {
	c.logger.Debugf(format, args...)
}

func (c *LogWrapper) Infof(format string, args ...interface{}) {
	switch format {
	case "rotate to %s", "received fake rotate event, next log name is %s", "rotate binlog to %s", "table structure changed, clear table cache: %s.%s\n":
		return
	}
	c.logger.Infof(format, args...)
}

func (c *LogWrapper) Warnf(format string, args ...interface{}) {
	c.logger.Warnf(format, args...)
}

func (c *LogWrapper) Errorf(format string, args ...interface{}) {
	// Noisy bug on close, can be ignored.
	// https://github.com/squareup/spirit/pull/65
	if len(args) == 1 {
		message := fmt.Sprintf("%s", args[0])
		if format == "canal start sync binlog err: %v" && message == "Sync was closed" {
			return
		}
	}
	c.logger.Errorf(format, args...)
}

func (c *LogWrapper) Fatalf(format string, args ...interface{}) {
	c.logger.Fatalf(format, args...)
}

func (c *LogWrapper) Debug(args ...interface{}) {
	c.logger.Debug(args...)
}

func (c *LogWrapper) Info(args ...interface{}) {
	c.logger.Info(args...)
}

func (c *LogWrapper) Warn(args ...interface{}) {
	c.logger.Warn(args...)
}

func (c *LogWrapper) Error(args ...interface{}) {
	c.logger.Error(args...)
}

func (c *LogWrapper) Fatal(args ...interface{}) {
	c.logger.Fatal(args...)
}

func (c *LogWrapper) Debugln(args ...interface{}) {
	c.logger.Debugln(args...)
}

func (c *LogWrapper) Infoln(args ...interface{}) {
	c.logger.Infoln(args...)
}

func (c *LogWrapper) Warnln(args ...interface{}) {
	c.logger.Warnln(args...)
}

func (c *LogWrapper) Errorln(args ...interface{}) {
	c.logger.Errorln(args...)
}

func (c *LogWrapper) Fatalln(args ...interface{}) {
	c.logger.Fatalln(args...)
}

func (c *LogWrapper) Panic(args ...interface{}) {
	c.logger.Panic(args...)
}

func (c *LogWrapper) Panicf(format string, args ...interface{}) {
	c.logger.Panicf(format, args...)
}

func (c *LogWrapper) Panicln(args ...interface{}) {
	c.logger.Panicln(args...)
}

func (c *LogWrapper) Print(args ...interface{}) {
	c.logger.Print(args...)
}

func (c *LogWrapper) Printf(format string, args ...interface{}) {
	c.logger.Printf(format, args...)
}

func (c *LogWrapper) Println(args ...interface{}) {
	c.logger.Println(args...)
}

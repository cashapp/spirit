package repl

import (
	"testing"

	"github.com/sirupsen/logrus"
)

func TestLogWrapper(t *testing.T) {
	logger := NewLogWrapper(logrus.New())
	logger.Debug("debug")
	logger.Info("info")
	logger.Warn("warn")
	logger.Error("error")
	logger.Debugln("debugln")
	logger.Infoln("infoln")
	logger.Warnln("warnln")
	logger.Errorln("errorln")
	logger.Infof("infof %s", "test")
	logger.Warnf("warnf %s", "test")
	logger.Errorf("errorf %s", "test")
	logger.Print("print")
	logger.Printf("printf %s", "test")
	logger.Println("println")
}

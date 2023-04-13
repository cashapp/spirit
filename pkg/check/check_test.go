package check

import (
	"context"
	"os"
	"testing"

	"github.com/siddontang/loggers"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func dsn() string {
	dsn := os.Getenv("MYSQL_DSN")
	if dsn == "" {
		return "msandbox:msandbox@tcp(127.0.0.1:8030)/test"
	}
	return dsn
}

func TestCheckAPI(t *testing.T) {
	testVal := "test"
	myfunc := func(
		_ context.Context,
		_ Resources,
		_ loggers.Advanced,
	) error {
		testVal = "newval"
		return nil
	}
	checkLen := len(checks)
	registerCheck("mycheck", myfunc, ScopeTesting)
	assert.Len(t, checks, checkLen+1)

	// Can't be duplicate registered because of a map
	registerCheck("mycheck", myfunc, ScopeTesting)
	assert.Len(t, checks, checkLen+1)

	assert.Equal(t, "test", testVal)
	err := RunChecks(context.Background(), Resources{}, logrus.New(), ScopeTesting)
	assert.NoError(t, err)
	assert.Equal(t, "newval", testVal)
}

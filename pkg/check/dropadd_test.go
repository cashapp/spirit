package check

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestDropAdd(t *testing.T) {
	r := Resources{
		Statement: "ALTER TABLE t.t1 DROP b, ADD b INT",
	}
	err := dropAddCheck(context.Background(), r, logrus.New())
	assert.Error(t, err)
	assert.ErrorContains(t, err, "column b is mentioned 2 times in the same statement")

	r.Statement = "ALTER TABLE t.t1 DROP b1, ADD b2 INT"
	err = dropAddCheck(context.Background(), r, logrus.New())
	assert.NoError(t, err)

	r.Statement = "bogus"
	err = dropAddCheck(context.Background(), r, logrus.New())
	assert.Error(t, err)
}

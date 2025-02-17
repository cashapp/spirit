package check

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestPrimaryKey(t *testing.T) {
	r := Resources{
		Statement: "ALTER TABLE t.t1 DROP PRIMARY KEY, ADD PRIMARY KEY (anothercol)",
	}
	err := primaryKeyCheck(context.Background(), r, logrus.New())
	assert.Error(t, err) // drop primary key

	r = Resources{
		Statement: "ALTER TABLE t.t1 ADD INDEX (anothercol)",
	}
	err = primaryKeyCheck(context.Background(), r, logrus.New())
	assert.NoError(t, err) // safe modification

	r = Resources{
		Statement: "gibberish",
	}
	err = primaryKeyCheck(context.Background(), r, logrus.New())
	assert.Error(t, err) // gibberish
}

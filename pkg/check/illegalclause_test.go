package check

import (
	"context"
	"testing"

	"github.com/cashapp/spirit/pkg/statement"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestIllegalClauseCheck(t *testing.T) {
	r := Resources{
		Statement: statement.MustNew("ALTER TABLE t1 ADD INDEX (b), ALGORITHM=INPLACE"),
	}
	err := illegalClauseCheck(context.Background(), r, logrus.New())
	assert.Error(t, err)
	assert.ErrorContains(t, err, "contains unsupported clause")

	r.Statement = statement.MustNew("ALTER TABLE t1  ADD c INT, ALGORITHM=INPLACE, LOCK=shared")
	err = illegalClauseCheck(context.Background(), r, logrus.New())
	assert.Error(t, err)
	assert.ErrorContains(t, err, "contains unsupported clause")

	r.Statement = statement.MustNew("ALTER TABLE t1  ADD c INT, lock=none")
	err = illegalClauseCheck(context.Background(), r, logrus.New())
	assert.Error(t, err)
	assert.ErrorContains(t, err, "contains unsupported clause")

	r.Statement = statement.MustNew("ALTER TABLE t1 engine=innodb, algorithm=copy")
	err = illegalClauseCheck(context.Background(), r, logrus.New())
	assert.Error(t, err)
	assert.ErrorContains(t, err, "contains unsupported clause")
}

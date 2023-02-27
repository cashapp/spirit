package migration

import (
	"context"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
)

func TestIsCompatible(t *testing.T) {
	cfg, err := mysql.ParseDSN(dsn())
	assert.NoError(t, err)

	runSQL(t, `DROP TABLE IF EXISTS compat1`)
	tbl := `CREATE TABLE compat1 (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		b varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	runSQL(t, tbl)
	assert.True(t, IsCompatible(context.TODO(), &Migration{
		Host:        cfg.Addr,
		Username:    cfg.User,
		Password:    cfg.Passwd,
		Database:    cfg.DBName,
		Concurrency: 16,
		Table:       "compat1",
		Alter:       "ENGINE=InnoDB",
	}))

	runSQL(t, `DROP TABLE IF EXISTS incompat1`)
	tbl = `CREATE TABLE incompat1 (

	   	id int(11) NOT NULL,
	   	name varchar(255) NOT NULL,
	   	b varchar(255) NOT NULL

	   )`
	runSQL(t, tbl)

	assert.False(t, IsCompatible(context.TODO(), &Migration{
		Host:        cfg.Addr,
		Username:    cfg.User,
		Password:    cfg.Passwd,
		Database:    cfg.DBName,
		Concurrency: 16,
		Table:       "incompat1", // no PK
		Alter:       "ENGINE=InnoDB",
	}))

	runSQL(t, `DROP TABLE IF EXISTS incompat2`)
	tbl = `CREATE TABLE incompat2 (

	   	id int(11) NOT NULL,
	   	name varchar(255) NOT NULL,
	   	b varchar(255) NOT NULL,
	   	PRIMARY KEY (b)

	   )`
	runSQL(t, tbl)

	assert.False(t, IsCompatible(context.TODO(), &Migration{
		Host:        cfg.Addr,
		Username:    cfg.User,
		Password:    cfg.Passwd,
		Database:    cfg.DBName,
		Concurrency: 16,
		Table:       "incompat2", // VARCHAR PK
		Alter:       "ENGINE=InnoDB",
	}))
}

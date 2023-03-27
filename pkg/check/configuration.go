package check

import (
	"context"
	"errors"

	"github.com/siddontang/loggers"
)

func init() {
	registerCheck("configuration", configurationCheck, ScopePreflight)
}

// check the configuration of the database. There are some hard nos,
// and some suggestions around configuration for performance.
func configurationCheck(ctx context.Context, r Resources, logger loggers.Advanced) error {
	var binlogFormat, innodbAutoincLockMode, binlogRowImage, logBin string
	err := r.DB.QueryRowContext(ctx, "SELECT @@global.binlog_format, @@global.innodb_autoinc_lock_mode, @@global.binlog_row_image, @@global.log_bin").Scan(&binlogFormat, &innodbAutoincLockMode, &binlogRowImage, &logBin)
	if err != nil {
		return err
	}
	if binlogFormat != "ROW" {
		return errors.New("binlog_format must be ROW")
	}
	if innodbAutoincLockMode != "2" {
		// This is strongly encouraged because otherwise running parallel threads is pointless.
		// i.e. on a test with 2 threads running INSERT INTO new SELECT * FROM old WHERE <range>
		// the inserts will run in serial when there is an autoinc column on new and innodbAutoincLockMode != "2"
		// This is the auto-inc lock. It won't show up in SHOW PROCESSLIST that they are serial.
		logger.Warn("innodb_autoinc_lock_mode != 2. This will cause the migration to run slower than expected because concurrent inserts to the new table will be serialized.")
	}
	if binlogRowImage != "FULL" {
		// This might not be required, but is the only option that has been tested so far.
		// To keep the testing scope reduced for now, it is required.
		return errors.New("binlog_row_image must be FULL")
	}
	if logBin != "1" {
		// This is a hard requirement because we need to be able to read the binlog.
		return errors.New("log_bin must be enabled")
	}
	return nil
}

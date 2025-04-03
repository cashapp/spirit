package repl

/*




func TestReplClientResumeFromImpossible(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)

	testutils.RunSQL(t, "DROP TABLE IF EXISTS replresumet1, replresumet2, _replresumet1_chkpnt")
	testutils.RunSQL(t, "CREATE TABLE replresumet1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE replresumet2 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _replresumet1_chkpnt (a int)") // just used to advance binlog

	t1 := table.NewTableInfo(db, "test", "replresumet1")
	assert.NoError(t, t1.SetInfo(context.TODO()))
	t2 := table.NewTableInfo(db, "test", "replresumet2")
	assert.NoError(t, t2.SetInfo(context.TODO()))

	logger := logrus.New()
	cfg, err := mysql2.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	client := NewClient(db, cfg.Addr, t1, t2, cfg.User, cfg.Passwd, &ClientConfig{
		Logger:          logger,
		Concurrency:     4,
		TargetBatchTime: time.Second,
	})
	client.SetPos(mysql.Position{
		Name: "impossible",
		Pos:  uint32(12345),
	})
	err = client.Run()
	assert.Error(t, err)
}

func TestReplClientResumeFromPoint(t *testing.T) {
	db, err := sql.Open("mysql", testutils.DSN())
	assert.NoError(t, err)

	testutils.RunSQL(t, "DROP TABLE IF EXISTS replresumepointt1, replresumepointt2")
	testutils.RunSQL(t, "CREATE TABLE replresumepointt1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE replresumepointt2 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")

	t1 := table.NewTableInfo(db, "test", "replresumepointt1")
	assert.NoError(t, t1.SetInfo(context.TODO()))
	t2 := table.NewTableInfo(db, "test", "replresumepointt2")
	assert.NoError(t, t2.SetInfo(context.TODO()))

	logger := logrus.New()
	cfg, err := mysql2.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	client := NewClient(db, cfg.Addr, t1, t2, cfg.User, cfg.Passwd, &ClientConfig{
		Logger:          logger,
		Concurrency:     4,
		TargetBatchTime: time.Second,
	})
	if dbconn.IsMySQL84(db) { // handle MySQL 8.4
		client.isMySQL84 = true
	}
	pos, err := client.getCurrentBinlogPosition()
	assert.NoError(t, err)
	pos.Pos = 4
	assert.NoError(t, client.Run())
	client.Close()
}

func TestReplClientOpts(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)

	testutils.RunSQL(t, "DROP TABLE IF EXISTS replclientoptst1, replclientoptst2, _replclientoptst1_chkpnt")
	testutils.RunSQL(t, "CREATE TABLE replclientoptst1 (a INT NOT NULL auto_increment, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE replclientoptst2 (a INT NOT NULL  auto_increment, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _replclientoptst1_chkpnt (a int)") // just used to advance binlog

	testutils.RunSQL(t, "INSERT INTO replclientoptst1 (a, b, c) SELECT NULL, 1, 1 FROM dual")
	testutils.RunSQL(t, "INSERT INTO replclientoptst1 (a, b, c) SELECT NULL, 1, 1 FROM replclientoptst1 a JOIN replclientoptst1 b JOIN replclientoptst1 c LIMIT 100000")
	testutils.RunSQL(t, "INSERT INTO replclientoptst1 (a, b, c) SELECT NULL, 1, 1 FROM replclientoptst1 a JOIN replclientoptst1 b JOIN replclientoptst1 c LIMIT 100000")
	testutils.RunSQL(t, "INSERT INTO replclientoptst1 (a, b, c) SELECT NULL, 1, 1 FROM replclientoptst1 a JOIN replclientoptst1 b JOIN replclientoptst1 c LIMIT 100000")
	testutils.RunSQL(t, "INSERT INTO replclientoptst1 (a, b, c) SELECT NULL, 1, 1 FROM replclientoptst1 a JOIN replclientoptst1 b JOIN replclientoptst1 c LIMIT 100000")

	t1 := table.NewTableInfo(db, "test", "replclientoptst1")
	assert.NoError(t, t1.SetInfo(context.TODO()))
	t2 := table.NewTableInfo(db, "test", "replclientoptst2")
	assert.NoError(t, t2.SetInfo(context.TODO()))

	logger := logrus.New()
	cfg, err := mysql2.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	client := NewClient(db, cfg.Addr, t1, t2, cfg.User, cfg.Passwd, &ClientConfig{
		Logger:          logger,
		Concurrency:     4,
		TargetBatchTime: time.Second,
	})
	assert.Equal(t, 0, db.Stats().InUse) // no connections in use.
	assert.NoError(t, client.Run())
	defer client.Close()

	// Disable key above watermark.
	client.SetKeyAboveWatermarkOptimization(false)

	startingPos := client.GetBinlogApplyPosition()

	// Delete more than 10000 keys so the FLUSH has to run in chunks.
	testutils.RunSQL(t, "DELETE FROM replclientoptst1 WHERE a BETWEEN 10 and 50000")
	assert.NoError(t, client.BlockWait(context.TODO()))
	assert.Equal(t, 49961, client.GetDeltaLen())
	// Flush. We could use client.Flush() but for testing purposes lets use
	// PeriodicFlush()
	go client.StartPeriodicFlush(context.TODO(), 1*time.Second)
	time.Sleep(2 * time.Second)
	client.StopPeriodicFlush()
	assert.Equal(t, 0, db.Stats().InUse) // all connections are returned

	assert.Equal(t, 0, client.GetDeltaLen())

	// The binlog position should have changed.
	assert.NotEqual(t, startingPos, client.GetBinlogApplyPosition())
}

func TestReplClientQueue(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)

	testutils.RunSQL(t, "DROP TABLE IF EXISTS replqueuet1, replqueuet2, _replqueuet1_chkpnt")
	testutils.RunSQL(t, "CREATE TABLE replqueuet1 (a VARCHAR(255) NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE replqueuet2 (a VARCHAR(255) NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _replqueuet1_chkpnt (a int)") // just used to advance binlog

	testutils.RunSQL(t, "INSERT INTO replqueuet1 (a, b, c) SELECT UUID(), 1, 1 FROM dual")
	testutils.RunSQL(t, "INSERT INTO replqueuet1 (a, b, c) SELECT UUID(), 1, 1 FROM replqueuet1 a JOIN replqueuet1 b JOIN replqueuet1 c LIMIT 100000")
	testutils.RunSQL(t, "INSERT INTO replqueuet1 (a, b, c) SELECT UUID(), 1, 1 FROM replqueuet1 a JOIN replqueuet1 b JOIN replqueuet1 c LIMIT 100000")
	testutils.RunSQL(t, "INSERT INTO replqueuet1 (a, b, c) SELECT UUID(), 1, 1 FROM replqueuet1 a JOIN replqueuet1 b JOIN replqueuet1 c LIMIT 100000")
	testutils.RunSQL(t, "INSERT INTO replqueuet1 (a, b, c) SELECT UUID(), 1, 1 FROM replqueuet1 a JOIN replqueuet1 b JOIN replqueuet1 c LIMIT 100000")

	t1 := table.NewTableInfo(db, "test", "replqueuet1")
	assert.NoError(t, t1.SetInfo(context.TODO()))
	t2 := table.NewTableInfo(db, "test", "replqueuet2")
	assert.NoError(t, t2.SetInfo(context.TODO()))

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	client := NewClient(db, cfg.Addr, t1, t2, cfg.User, cfg.Passwd, NewClientDefaultConfig())
	assert.NoError(t, client.Run())
	defer client.Close()

	copier, err := row.NewCopier(db, t1, t2, row.NewCopierDefaultConfig())
	assert.NoError(t, err)
	// Attach copier's keyabovewatermark to the repl client
	client.KeyAboveCopierCallback = copier.KeyAboveHighWatermark

	assert.NoError(t, copier.Open4Test()) // need to manually open because we are not calling Run()

	// Delete from the table, because there is no keyabove watermark
	// optimization these deletes will be queued immediately.
	testutils.RunSQL(t, "DELETE FROM replqueuet1 LIMIT 1000")
	assert.NoError(t, client.BlockWait(context.TODO()))
	assert.Equal(t, 1000, client.GetDeltaLen())

	// Read from the copier
	chk, err := copier.Next4Test()
	assert.NoError(t, err)
	prevUpperBound := chk.UpperBound.Value[0].String()
	assert.Equal(t, "`a` < "+prevUpperBound, chk.String())
	// read again
	chk, err = copier.Next4Test()
	assert.NoError(t, err)
	assert.Equal(t, fmt.Sprintf("`a` >= %s AND `a` < %s", prevUpperBound, chk.UpperBound.Value[0].String()), chk.String())

	// Accumulate more deltas
	testutils.RunSQL(t, "INSERT INTO replqueuet1 (a, b, c) SELECT UUID(), 1, 1 FROM replqueuet1 LIMIT 501")
	assert.NoError(t, client.BlockWait(context.TODO()))
	assert.Equal(t, 1501, client.GetDeltaLen())

	// Flush the changeset
	assert.NoError(t, client.Flush(context.TODO()))
	assert.Equal(t, 0, client.GetDeltaLen())

	// Accumulate more deltas
	testutils.RunSQL(t, "DELETE FROM replqueuet1 LIMIT 100")
	assert.NoError(t, client.BlockWait(context.TODO()))
	assert.Equal(t, 100, client.GetDeltaLen())

	// Final flush
	assert.NoError(t, client.Flush(context.TODO()))
	assert.Equal(t, 0, client.GetDeltaLen())
}

func TestFeedback(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)

	testutils.RunSQL(t, "DROP TABLE IF EXISTS feedbackt1, feedbackt2, _feedbackt1_chkpnt")
	testutils.RunSQL(t, "CREATE TABLE feedbackt1 (a VARCHAR(255) NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE feedbackt2 (a VARCHAR(255) NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _feedbackt1_chkpnt (a int)") // just used to advance binlog

	t1 := table.NewTableInfo(db, "test", "replqueuet1")
	assert.NoError(t, t1.SetInfo(context.TODO()))
	t2 := table.NewTableInfo(db, "test", "replqueuet2")
	assert.NoError(t, t2.SetInfo(context.TODO()))

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	client := NewClient(db, cfg.Addr, t1, t2, cfg.User, cfg.Passwd, NewClientDefaultConfig())
	assert.NoError(t, client.Run())
	defer client.Close()

	// initial values expected:
	assert.Equal(t, time.Millisecond*500, client.targetBatchTime)
	assert.Equal(t, int64(1000), client.targetBatchSize)

	// Make it complete 5 times faster than expected
	// Run 9 times initially.
	for range 9 {
		client.feedback(1000, time.Millisecond*100)
	}
	assert.Equal(t, int64(1000), client.targetBatchSize) // no change yet
	client.feedback(0, time.Millisecond*100)             // no keys, should not cause change.
	assert.Equal(t, int64(1000), client.targetBatchSize) // no change yet
	client.feedback(1000, time.Millisecond*100)          // 10th time.
	assert.Equal(t, int64(5000), client.targetBatchSize) // 5x more keys.

	// test with slower chunk
	for range 10 {
		client.feedback(1000, time.Second)
	}
	assert.Equal(t, int64(500), client.targetBatchSize) // less keys.

	// Test with a way slower chunk.
	for range 10 {
		client.feedback(500, time.Second*100)
	}
	assert.Equal(t, int64(5), client.targetBatchSize) // equals the minimum.
}

// TestBlockWait tests that the BlockWait function will:
// - check the server's binary log position
// - block waiting until the repl client is at that position.
func TestBlockWait(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)

	testutils.RunSQL(t, "DROP TABLE IF EXISTS blockwaitt1, blockwaitt2, _blockwaitt1_chkpnt")
	testutils.RunSQL(t, "CREATE TABLE blockwaitt1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE blockwaitt2 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _blockwaitt1_chkpnt (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")

	t1 := table.NewTableInfo(db, "test", "blockwaitt1")
	assert.NoError(t, t1.SetInfo(context.TODO()))
	t2 := table.NewTableInfo(db, "test", "blockwaitt2")
	assert.NoError(t, t2.SetInfo(context.TODO()))

	logger := logrus.New()
	cfg, err := mysql2.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	client := NewClient(db, cfg.Addr, t1, t2, cfg.User, cfg.Passwd, &ClientConfig{
		Logger:          logger,
		Concurrency:     4,
		TargetBatchTime: time.Second,
	})
	assert.NoError(t, client.Run())
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2) // if it takes any longer block wait is failing.
	defer cancel()

	assert.NoError(t, client.BlockWait(ctx))

	// Insert into t1.
	testutils.RunSQL(t, "INSERT INTO blockwaitt1 (a, b, c) VALUES (1, 2, 3)")
	assert.NoError(t, client.Flush(ctx))                                      // apply the changes (not required, they only need to be received for block wait to unblock)
	assert.NoError(t, client.BlockWait(ctx))                                  // should be quick still.
	testutils.RunSQL(t, "INSERT INTO blockwaitt1 (a, b, c) VALUES (2, 2, 3)") // don't apply changes.
	assert.NoError(t, client.BlockWait(ctx))                                  // should be quick because apply not required.

	testutils.RunSQL(t, "ANALYZE TABLE blockwaitt1")
	testutils.RunSQL(t, "ANALYZE TABLE blockwaitt1")
	testutils.RunSQL(t, "ANALYZE TABLE blockwaitt1")
	testutils.RunSQL(t, "ANALYZE TABLE blockwaitt1")
	testutils.RunSQL(t, "ANALYZE TABLE blockwaitt1")
	assert.NoError(t, client.BlockWait(ctx)) // should be quick
}

*/

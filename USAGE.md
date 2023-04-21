# How to use Spirit

To create a binary:

```
cd cmd/spirit
go build
./spirit --help
```

## Configuration

### host

- Type: String
- Default value: `localhost:3306`
- Examples: `mydbhost`, `mydbhost:3307`

The host (and optional port) to use when connecting to MySQL.

### username

- Type: String
- Default value: `msandbox`

The username to use when connecting to MySQL.

### password

- Type: String
- Default value: `msandbox`

The password to use when connecting to MySQL.

### database

- Type: String
- Default value: `test`

The database that the schema change will be performed in.

### table

- Type: String
- Default value: `stock`

The table that the schema change will be performed on.

### alter

- Type: String
- Default value: `engine=innodb`
- Examples: `add column foo int`, `add index foo (bar)`

The alter table command to perform. The default value is a _null alter table_, which can be useful for testing.

### threads

- Type: Integer
- Default value: `4`
- Range: `1-64`

The number of parallel threads for use during both the copy and checksum phases of the schema change.

You may want to wrap this in some automation and set it to a percentage of the cores of your database server. For example, if you have a 32-core machine you may choose to set this to `8`. Approximately 25% is a good starting point, making sure you always leave plenty of free cores for regular database operations. If your migration is IO bound and/or your IO latency is high (such as Aurora) you may even go higher than 25%.

Note that Spirit does not support dynamically adjust the number of threads while running, but it does support automatically resuming from a checkpoint if it is killed. This means that if you find that you've misjudged the number of threads (or [target-chunk-time](#target-chunk-time)), you can simply kill the Spirit process and start it again with a different thread value.

### target-chunk-time

- Type: Duration
- Default value: `500ms`
- Range: `100ms-5s`
- Typical safe values: `100ms-1s`

The target time for each copy or checksum operation. Note that the chunk size is specified as a _target time_ and not a _target rows_. This is helpful because rows can be inconsistent when you consider some tables may have a lot of columns or secondary indexes, or copy tasks may slow down as the workload becomes IO bound.

The target is not a hard limit, but rather a guideline which is recalculated based on a 90th percentile from the last 10 chunks that were copied. You should expect some outliers where the copy time is higher than the target. Outliers >5x the target will print to the log, and force an immediate reduction in how many rows are copied per chunk without waiting for the next recalculation.

Larger values generally yield better performance, but have consequences:
- A `5s` value means that at any point replicas will appear `5s` behind the source. Spirit does not support read-replicas, so we do not typically consider this a problem. See [replica-max-lag](#replica-max-lag) for more context.
- Data locks (row locks) are held for the duration of each transaction, so even a `1s` chunk may lead to frustrating user experiences. Consider the scenario that a simple update query usually takes `<5ms`. If it tries to update a row that has just started being copied it will now take approximately `1.005s` to complete. In scenarios where there is a lot of contention around a few rows, this could even lead to a large backlog of queries waiting to be executed.
- It is recommended to set the target chunk time to a value for which if queries increased by this much, user experience would still be acceptable even if a little frustrating. In some of our systems this mean up to `2s`. We do not know of scenarios where values should ever exceed `5s`. If you can tolerate more unavailability, consider running DDL directly on the MySQL server.

### attempt-inplace-ddl

- Type: Boolean
- Default value: FALSE

When set to `TRUE` Spirit will attempt to perform the schema change using MySQL's `INPLACE` algorithm, before falling back to performing it's usual copy process. `INPLACE` is non-blocking on the system where the DDL was initiated, but it will block on binary-log based read replicas. This means it's typically only safe to enable if you have no read replicas, or your read replicas are based on physical log shipping (i.e. Aurora).

### checksum

- Type: Boolean
- Default value: TRUE

When set to `TRUE` Spirit will perform a checksum of the data in the table after the copy phase. This is a good way to ensure that the copy phase was successful, but it does add some overhead to the process. When you resume-from-checkpoint, Spirit will only run with the checksum enabled (regardless of your configuration). This is because it can not rely on duplicate-key errors to detect issues in the copy phase if the DDL included adding a new `UNIQUE` key.

The checksum typically adds about 10-20% of additional time to the migration, but it is recommended to always leave it enabled. A failed checksum will cause spirit to exit with a non-zero exit code, and means that there is either:
- A bug in Spirit
- A bug in MySQL
- Hardware errors

In testing, the checksum feature has identified corruption issues on desktops with non ECC memory. You may believe that this is what the InnoDB page checksums are for, but they are more specifically for detecting corruption introduced from the IO layer. Memory based corruption is not detected and remains common.

### replica-dsn

- Type: String
- Default value: ``
- Example: `root:mypassword@tcp(localhost:3307)/test`

Used in combination with [replica-max-lag](#replica-max-lag). This is the host which Spirit will connect to to determine if the copy should be throttled to ensure replica health.

### replica-max-lag

- Type: Duration
- Default value: `120s`
- Range: `10s-1hr`

Used in combination with [replica-dsn](#replica-dsn). This is the maximum lag that the replica is allowed to have before Spirit will throttle the copy phase to ensure that the replica does not fall too far behind. Spirit **does not support read-replicas** and throttling is only intended to ensure that replicas do not fall so far behind that disaster recovery will be affected. If you require a high fidelity for replicas, you should consider using `gh-ost` instead of Spirit.

It is recommended that you use Spirit in combination with either parallel replication (which gets much better in MySQL 8.0) or non-binary log based replicas such as Aurora. If you are using the default single threaded replication and specifying a `replica-dsn` + `replica-max-lag`, you should expect to constantly be throttled.



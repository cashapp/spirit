# What is this?

Spirit is a _clone_ of the schema change tool [gh-ost](https://github.com/github/gh-ost).

It works very similar to gh-ost except:
- It only supports MySQL 5.7 and MySQL 8.0 (it is anticipated only 8.0 will be supported once Aurora v2 reaches end of life)
- It is multi-threaded in both the row-copying and the binlog applying phase
- It supports resume-from-checkpoint

The goal of spirit is to apply schema changes much faster than gh-ost. This makes it unsuitable in the following scenarios:
- You use read-replicas to serve traffic
- You have tables with `VARCHAR` primary keys
- You require support for older versions of MySQL

If this is the case, `gh-ost` remains a fine choice.

See [USAGE](usage.md) for more information on how to use spirit.

## Optimizations

The following are some of the optimizations that make spirit faster than gh-ost:

### Dynamic Chunking

Rather than accept a fixed chunk size (such as 1000 rows), spirit instead takes a target chunk time (such as 2s). It then dynamically adjusts the chunk size to meet this target. This is both safer for very wide tables with a lot of indexes and faster for smaller tables.

2s is quite "high" for traditional MySQL environments, but remember _spirit does not support read-replicas_. This helps it copy chunks as efficiently as possible.

### Ignore Key Above Watermark

As spirit is copying rows, it keeps track of the highest key-value that either has been copied, or could be in the process of being copied. This is called the "high watermark". As rows are discovered from the binary log, they can be discarded if the key is above the high watermark. This is because once the copier reaches this point, it is guaranteed it will copy the latest version of the row.

In practice, this optimization works really well when your table has a `auto_increment` `PRIMARY KEY` and most of the inserts or modifications are at the end of the table.

**Note:** Spirit does not support `VARCHAR` primary keys, so it does not need to worry about collation issues when comparing if a key is above another key.

### Change Row Map

As spirit discovers rows that have been changed via the binary log, it stores them in a map. Or rather, it stores the key, and if the last operation was a `DELETE` or any other operation. This is called the "change row map". Periodically it then flushes the change row map by batching a large `REPLACE INTO new_table .. SELECT FROM old_table` and `DELETE FROM new_table WHERE pk IN (..)` statement.

In some workloads this can result in significant performance improvements, because updates from the binary log are merged and de-duplicated. i.e. if a row is updated 10 times, it will only be copied once.

**Note:** Spirit does not support `VARCHAR` primary keys, so it does not need to worry about collations where 'a' != 'A' in the change row map.

### Multi-threaded copy

Spirit will copy rows in multiple threads. This optimization really requires MySQL 8.0+ to make sense, which has much better support for multi-threaded replication.

While spirit does not support read-replicas, it still tries to keep replication mostly up to date (with support for reading a replica every 2 seconds and observing lag). The replication monitor is not intended to be as high fidelity as gh-ost, and only used to ensure that DR functionality is not impacted.

### Attempt Instant DDL

Spirit will attempt to use MySQL 8.0's `INSTANT` DDL assertion before applying the change itself. If the DDL change supports it, `INSTANT DDL` is a very fast operation and only requires a metadata change.

**Note:** This feature has been contributed to `gh-ost` by the same authors of Spirit. It is disabled by default, and [only in the master branch](https://github.com/github/gh-ost/blob/master/doc/command-line-flags.md#attempt-instant-ddl).

## Performance

Spirit uses a default of 4 threads and a chunk-target of 500ms. Tests performed on an m1 mac with 10 cores and MySQL 8.0.31:

| Table/Scenario                               | Gh-ost   | spirit  | spirit (checksum disabled) |
| -------------------------------------------- | -------- | ------- | -------------------------- |
| finch.balances (800MB/1M rows), idle load    | 28.720s  | 11.197s | 9.278s                     |
| finch.balances (800MB/1M rows), during bench | 2:50m+   | ~15-18s | ~15-18s                    |

Notes:

* Tests performed using the [finch](https://github.com/square/finch) benchmarking tool.
* Times during benchmarks vary a lot more. It doesn't seem fair to state a number when there is so much variance, so that is why you see a range.
* The checksum is a step that spirit performs that gh-ost does not! The checksum disabled compares apples-to-apples, but leaving it enabled is recommended.
* For the "during bench" test, the migration was started as soon as the load phase of the benchmark was finished. The bench duration was 600s.
* The benchmark might be a worse case for gh-ost, since it prioritizes replication over row-copying and the benchmark never lets up. Spirit takes the opposite approach, and applies replication changes much later.

## Risks and Limitations

Writing a new data migration tool is scary, since bugs have real consequences (data loss). These are the main problems we anticipate:

1. The cut-over algorithm is not as battle tested as gh-ost's. We have studied it in detail, and believe it's comparable. We believe we've managed to mitigate the other risks up until cut-over with the introduction of a checksum feature, which is enabled by default.
2. Spirit does not support as many different table types as gh-ost. Currently, primary keys can be int/bigint \[unsigned\] or varbinary. Composite primary keys are still supported, but there are currently no plans to support `VARCHAR` primary keys.
3. We have tried to balance making Spirit _as fast as possible_ while still being safe to run on production systems that are running existing workloads. Sometimes this means spirit might venture into creating slow downs in application performance. If it does, please file an issue and help us make improvements.
# What is this?

Spirit is a _clone_ of the schema change tool [gh-ost](https://github.com/github/gh-ost).

It works very similar to gh-ost except:
- It only supports MySQL 5.7 and MySQL 8.0 (it is anticipated only 8.0 will be supported once Aurora v2 reaches end of life)
- It it multi-threaded in the row-copying phase
- It supports resume-from-checkpoint

The goal of spirit is to apply schema changes much faster than gh-ost. This makes it unsuitable in the following scenarios:
- You use read-replicas to serve traffic
- You have tables with `VARCHAR` primary keys
- You require support for older versions of MySQL

If this is the case, `gh-ost` remains a fine choice.

## Optimizations

The following are some of the optimizations that make spirit faster than gh-ost:

### Dynamic Chunking

Rather than accept a fixed chunk size (such as 1000 rows), spirit instead takes a target chunk time (such as 500ms). It then dynamically adjusts the chunk size to meet this target. This is both safer for very wide tables with a lot of indexes and faster for smaller tables.

500ms is quite "high" for traditional MySQL environments, but remember _spirit does not support read-replicas_. This helps it copy chunks as efficiently as possible.

### Ignore Key Above Watermark

As spirit is copying rows, it keeps track of the highest key-value that either has been copied, or could be in the process of being copied. This is called the "high watermark". As rows are discovered from the binary log, they can be discarded if the key is above the high watermark. This is because once the copier reaches this point, it is guaranteed it will copy the latest version of the row.

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

1. The cut-over algorithm is not as battle tested as gh-ost's. We have studied it in detail, and believe it's comparable.
3. The optimal configuration (i.e. number of threads, chunk-target-in-ms) is not well understood and doesn't scale based on DB instance size. Since the goal is to be more aggressive than gh-ost, there may be some real-world cases where performance is impacted quite significantly while running Spirit.
4. The chunker expands ranges if the estimated rows is lower than the logical space between min/max key. This is disabled for auto_inc keys, since it's likely there are some areas where there aren't gaps in the sequence, and chunks could be very slow. Maybe disabling just for this case is a bad idea, since we hide other cases where dynamic expanding ranges is not good. In the case of auto-inc keys, this could mean very slow migrations where there is a large gap between min/max key. In other cases it could mean stalls as a lot of keys are concentrated in one area.
5. It does not support as many different table types as gh-ost. Currently, primary keys can be int/bigint \[unsigned\] or varbinary. Composite primary keys are supported, but it's currently not planned to support `VARCHAR` primary keys.
# About Chunkers

Chunkers are used to split a table into multiple chunks for copying. The downside of a chunk being too large is as follows:
- Replicas fall behind the master (we don't support read-replicas, but at a certain point it impacts DR).
- Locks are held for the duration of a chunk copy, so a large chunk can block other operations on the table.

All chunkers support "dynamic chunking" which is our way of saying that from a configuration perspective you specify the ideal chunk size in time-based units (e.g. 500ms) and the chunker will adjust the chunk size to try to meet that target. This tends to be a better approach than specifying a fixed chunk size, because the chunk size can vary wildly depending on the table. As the _new table gets larger, we typically see the chunk size reduce significantly to compensate for larger insert times. We believe this is more likely to occur on Aurora than MySQL because on IO bound workloads it does not have the [change buffer](https://dev.mysql.com/doc/refman/8.0/en/innodb-change-buffer.html).

Our thoughts are that spirit should be aggressive in copying, but there should only be minimal elevation in p99 response times. If you consider that a table regularly has DML queries that take 1-5ms, then it is reasonable to assume a chunk-time of 500ms will elevate some query to 505ms. Assuming this contention is limited, it may only be observed by the pMax and not the p99. It is usually application-dependent how much of a latency hit is acceptable. Our belief is that 500ms is on the high-end of acceptable for defaults, and users will typically lower it rather than increase it. We limit the maximum chunk time to 5s because it is unlikely that users can tolerate larger than a 5s latency hit for a single query on an OLTP system. Since we also adjust various lock wait timeouts based on the assumption that chunks are about this size, increasing beyond 5s would require additional tuning.

Chunking becomes a complicated problem because data can have an uneven distribution, and some tables have composite or strange data types for PRIMARY KEYs. We have chosen to solve the chunking problem by not using a one-size-fits-all approach, but rather an interface that has two current implementations: `composite` and `optimistic`.

## Composite Chunker

The composite chunker is our newest chunker, and it is selected **unless** the table has an `AUTO_INCREMENT` single-column `PRIMARY KEY`.

Its implementation is very similar to how the chunker in gh-ost works:
- A `SELECT` statement is performed to find the exact PK value of the row that is `chunkSize` larger than the current chunk pointer:
```
SELECT pk FROM table WHERE pk > chunkPointer ORDER BY pk LIMIT 1 OFFSET {chunkSize}
```
- An `INSERT .. SELECT` statement is run on the table to copy between the last chunk pointer and the new value.

The composite chunker is very good at dividing the chunks up equally, since sparing a brief race condition each chunk will match exactly the `chunkSize` value. The main downside is that it becomes a little bit wasteful when you have an `auto_increment` `PRIMARY KEY`s and rarely delete data. In this case, you waste the initial `SELECT` statement, since the client could easily calculate the next chunk pointer by adding `chunkSize` to the previous chunk pointer. A second issue is that the composite chunker always returns `FALSE` for the KeyAboveHighWatermark optimization. It is possible that it could be implemented correctly in future, but for code-simplicity we have chosen not to for now.

Many of our use-cases have `auto_increment` `PRIMARY KEY`s, so despite the composite chunker also being able to support non-composite `PRIMARY KEY`s, we have no plans to switch to it entirely.

## Optimistic Chunker

The optimistic chunker was our first chunker, and it's ideal for one of our main use case: `AUTO_INCREMENT` `PRIMARY KEY`s.

Its basic implementation is as follows:
- Find the min/max of the `PRIMARY KEY` column.
- Have a special chunk retrieve values less than the min value*
- Advance by chunk size until you reach the max value.
- Have a special chunk retrieve values greater than the max value*

\* Because the value is cached, it's possible a small number of rows exist, particularly at the end of the table.

To a certain extent, the chunk-size will automatically adjust to small gaps in the table as dynamic chunking adjusts to compensate for slightly faster copies. However, this is intentionally limited with dynamic chunking having a hard limit on the chunk size of `100K` rows. It can also only expand the chunk-size by 50% at a time. This helps prevent the scenario that quickly processed chunks (likely caused by table gaps) expand the chunk size too quickly, causing future chunks to be too large and causing QoS issues. 

To deal with large gaps, the optimistic chunker also supports a special "prefetching mode". The prefetching mode is enabled when the chunk size has already reached the `100K` limit, and each chunk is still only taking 20% of the target time for chunk copying. Prefetching was first developed when we discovered a user with ~20 million rows in the table but a big gap between the auto_increment value of 20 million and the end of the table (300 billion). You can think of the prefetching mode as not that much different from how the composite chunker works, as it will perform a SELECT query to find the next PK value it should use as a pointer. Prefetching is automatically disabled again if the chunk size is ever reduced below the `100K` limit.

### Future work 

The following are known issues:

* Neither of the chunkers support chunking on anything other than the `PRIMARY KEY`. For our purposes, this is usually acceptable but we may have to revisit this in the future.
* Not specifically a limitation of chunking, but some optimizations in spirit require that the `PRIMARY KEY` not have collations. Thus, we explicitly disallow `VARCHAR` primary keys in the chunker. The optimizations are very useful, which makes this a complex problem to fix. But it also appears to be the most common incompatibility issue with spirit, so we may have to reconsider this at some point.


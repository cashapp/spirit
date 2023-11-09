#!/bin/bash

go build ./cmd/spirit
~/sandboxes/msb_8_0_28/use test -e "DROP TABLE IF EXISTS t1; CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY auto_increment, b INT NOT NULL, c INT NOT NULL)"
./spirit --host "127.0.0.1:8028" --database=test --table=t1
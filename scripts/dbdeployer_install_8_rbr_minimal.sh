#!/bin/bash
set -xe

sudo apt install -y libncurses5

go install github.com/datacharmer/dbdeployer@latest

dbdeployer init
dbdeployer deploy single 8.0.33

echo "binlog_row_image=MINIMAL" >> ~/sandboxes/msb_8_0_33/my.sandbox.cnf

~/sandboxes/msb_8_0_33/restart
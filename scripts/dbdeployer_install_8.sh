#!/bin/bash
set -xe

sudo apt install -y libncurses5

go install github.com/datacharmer/dbdeployer@latest

dbdeployer init
dbdeployer deploy replication 8.0.33

cat ~/sandboxes/rsandbox_8_0_33/sbdescription.json
#!/bin/bash
set -xe

sudo apt install -y libncurses5

go install github.com/datacharmer/dbdeployer@latest

dbdeployer init

dbdeployer downloads get mysql-5.7.34-linux-glibc2.12-x86_64.tar.gz
dbdeployer unpack mysql-5.7.34-linux-glibc2.12-x86_64.tar.gz
dbdeployer deploy replication 5.7.34

echo "log-slave-updates" >> ~/sandboxes/rsandbox_5_7_34/master/my.sandbox.cnf

~/sandboxes/rsandbox_5_7_34/restart_all

cat ~/sandboxes/rsandbox_5_7_34/sbdescription.json

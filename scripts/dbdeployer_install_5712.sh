#!/bin/bash
set -xe

sudo apt install -y libncurses5

go install github.com/datacharmer/dbdeployer@latest

dbdeployer init

wget https://downloads.mysql.com/archives/get/p/23/file/mysql-5.7.12-linux-glibc2.5-x86_64.tar.gz
dbdeployer unpack mysql-5.7.12-linux-glibc2.5-x86_64.tar.gz
dbdeployer deploy single 5.7.12

# For spirit to work, binary logging must be enabled.
# It is not by default in MySQL 5.7

echo "log-bin" >> ~/sandboxes/msb_5_7_12/my.sandbox.cnf
echo "log-slave-updates" >> ~/sandboxes/msb_5_7_12/my.sandbox.cnf
echo "server-id=123" >> ~/sandboxes/msb_5_7_12/my.sandbox.cnf

~/sandboxes/msb_5_7_12/restart

#!/bin/bash
set -xe

sudo apt install -y libncurses5

go install github.com/datacharmer/dbdeployer@latest

dbdeployer init
wget https://dev.mysql.com/get/Downloads/MySQL-8.2/mysql-8.2.0-linux-glibc2.28-x86_64.tar.xz
dbdeployer unpack mysql-8.2.0-linux-glibc2.28-x86_64.tar.xz
dbdeployer deploy single 8.2.0

cat ~/sandboxes/msb_8_2_0/sbdescription.json
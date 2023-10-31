#!/bin/bash
set -xe

sudo apt install -y libncurses5

go install github.com/datacharmer/dbdeployer@latest

dbdeployer init
wget https://downloads.mysql.com/archives/get/p/23/file/mysql-8.1.0-linux-glibc2.28-x86_64.tar.xz
dbdeployer unpack mysql-8.1.0-linux-glibc2.28-x86_64.tar.xz
dbdeployer deploy single 8.1.0

cat ~/sandboxes/msb_8_1_0/sbdescription.json
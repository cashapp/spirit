#!/bin/bash
set -xe

sudo apt install -y libncurses5

go install github.com/datacharmer/dbdeployer@latest

dbdeployer init

dbdeployer downloads get mysql-8.0.28-linux-glibc2.17-x86_64-minimal.tar.xz
dbdeployer unpack mysql-8.0.28-linux-glibc2.17-x86_64-minimal.tar.xz

dbdeployer deploy single 8.0.28


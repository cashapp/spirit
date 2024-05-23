#!/bin/bash

set -xe

source "${0%/*}"/manual_install.sh

version=8.4.0

pushd /tmp 

setup
activate "$version"
deploy_replication

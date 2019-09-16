#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail

dlv_dir="$(pwd)/_output/dlv"
mkdir -p ${dlv_dir} || true

docker run --rm -it -v $dlv_dir:/root/go/bin centos:7 bash -c "yum install -y epel-release && yum install -y git go && go get -u github.com/go-delve/delve/cmd/dlv"

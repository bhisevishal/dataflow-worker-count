#!/bin/bash
set -e

(
  set -x

  rm -f ./dataflow_worker_count

  go version
  go mod init dataflow_worker_count || true
  go mod tidy
  go get -u ./

  go build -o ./dataflow_worker_count
)

./dataflow_worker_count --help

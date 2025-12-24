#!/bin/bash
# shellcheck disable=SC2164
cd "$(dirname "$0")"

export GOPATH=~/go

go install mvdan.cc/gofumpt@latest
go install github.com/segmentio/golines@latest
"$GOPATH"/bin/golines -w .
"$GOPATH"/bin/gofumpt -l -w .
npx prettier --write .

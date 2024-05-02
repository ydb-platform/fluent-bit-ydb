# `fluent-bit-ydb` - [Fluent Bit](https://fluentbit.io) [output](https://docs.fluentbit.io/manual/concepts/data-pipeline/output) for [YDB](https://github.com/ydb-platform/ydb).

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/ydb-platform/ydb/blob/main/LICENSE)
[![Release](https://img.shields.io/github/v/release/ydb-platform/fluent-bit-ydb.svg?style=flat-square)](https://github.com/ydb-platform/fluent-bit-ydb/releases)
[![PkgGoDev](https://pkg.go.dev/badge/github.com/ydb-platform/fluent-bit-ydb)](https://pkg.go.dev/github.com/ydb-platform/fluent-bit-ydb)
![tests](https://github.com/ydb-platform/fluent-bit-ydb/workflows/tests/badge.svg?branch=main)
![lint](https://github.com/ydb-platform/fluent-bit-ydb/workflows/lint/badge.svg?branch=main)
[![Go Report Card](https://goreportcard.com/badge/github.com/ydb-platform/fluent-bit-ydb)](https://goreportcard.com/report/github.com/ydb-platform/fluent-bit-ydb)
[![codecov](https://codecov.io/gh/ydb-platform/fluent-bit-ydb/badge.svg?precision=2)](https://app.codecov.io/gh/ydb-platform/fluent-bit-ydb)
![Code lines](https://sloc.xyz/github/ydb-platform/fluent-bit-ydb/?category=code)
[![Telegram](https://img.shields.io/badge/chat-on%20Telegram-2ba2d9.svg)](https://t.me/ydb_en)
[![WebSite](https://img.shields.io/badge/website-ydb.tech-blue.svg)](https://ydb.tech)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](https://github.com/ydb-platform/fluent-bit-ydb/blob/main/CONTRIBUTING.md)

## Build

```makefile
BIN=out_ydb.so make build
```

# Usage 

`fluent-bit -e out_ydb.so -c examples/tail2ydb.conf`

Configuration file (there is an example in files):
```
ConnectionURL - connection url for YDB
Certificates - path to file with certificates or certificate content
TablePath - relative table path
```

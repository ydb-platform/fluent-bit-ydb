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

Build prerequisites:

* [Golang](https://go.dev/dl/) v1.21 or later
* C compiler and linker suitable for the operating system used (needed to build the plugin's shared library)
* `make` utility

To build the plugin, run the following command:

```bash
BIN=out_ydb.so make build
```

## Configuration

The plugin supports the following configuration settings:

| Parameter     | Description |
|---------------|-------------|
| ConnectionURL | YDB connection URL, including the protocol, endpoint and database path (see the [documentation](https://ydb.tech/docs/en/concepts/connect)) |
| TablePath | Relative table path, may include the schema in form `SchemaName/TableName` |
| Columns | JSON structure mapping the fields of FluentBit record to the columns of target YDB table. May include the pseudo-fields listed below |
| CredentialsAnonymous | Configure as `1` for anonymous YDB authentication |
| CredentialsYcServiceAccountKeyFile | Set to the path of file containing the service account (SA) key, to use the SA key YDB authentication |
| CredentialsYcServiceAccountKeyJson | Set to the JSON data of the service account (SA) key instead of the filename (useful in K8s environment) |
| CredentialsYcMetadata | Configure as `1` for virtual machine metadata YDB authentication |
| CredentialsStatic | Username and password for YDB authentication, specified in the following format: `username:password@` |
| CredentialsToken | Custom token value, to use the token authentication YDB mode |
| Certificates | Path to the certificate authority (CA) trusted certificates file, or the literal trusted CA certificate value |
| LogLevel | Plugin specific logging level, should be one of `disabled`, `trace`, `debug`, `info`, `warn`, `error`, `fatal` or `panic` |

The following pseudo-fields are available, in addition to those available in the FluentBit record, to be mapped into the YDB table columns:

* `.timestamp` - record's timestamp, mandatory
* `.input` - record's input stream name, mandatory
* `.hash` - uint64 hash value computed over all the data fields (except the pseudo-fields), optional
* `.other` - the JSON document containing all the data fields which were not explicitly mapped to a field in the table, optional

## Usage example 

YDB database should be available, either in the form of a local single-node setup (see the [Quickstart](https://ydb.tech/docs/en/quickstart) section in YDB Documentation), a fully [managed service](https://yandex.cloud/en/services/ydb), or as part of the YDB cluster installed on self-hosted resources.

FluentBit should be installed, either version 2 or 3.

In the [examples](./examples/) directory the following files are provided:

* [tail2ydb.sql](./examples/tail2ydb.sql) - example of YDB table structure to capture the log;
* [tail2ydb.conf](./examples/tail2ydb.conf) - example of FluentBit configuration to read from `/var/log/syslog` and write to YDB table;
* [docker-compose.yml](./examples/docker-compose.yml) - Docker Compose setup to run the single-node YDB instance for development or testing purposes.

Target table should be created in the YDB database prior to running FluentBit with the configuraton referencing it.

To run the example configuration, customize the YDB connection settings in the `tail2ydb.conf` file, and run the following command:

```bash
fluent-bit -e out_ydb.so -c examples/tail2ydb.conf
```

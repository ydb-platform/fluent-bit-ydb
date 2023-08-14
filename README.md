# fluent-bit-ydb
Fluent-Bit go YDB output plugin

This is alpha version, use at your own risk.

Build:
```makefile
export BIN=ydb_plugin.so
make build
```

Use with fluent-bit:

`fluent-bit -e ydb_plugin.so -c examples/flb_example.conf`

Configuration file (there is an example in files):
```
ConnectionURL - connection url for YDB
TableName - table name for logs.
TablePath - table path with DB name.


YDB_ANONYMOUS_CREDENTIALS 1
YDB_SERVICE_ACCOUNT_KEY_CREDENTIALS 123456
YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS 1.key
YDB_ACCESS_TOKEN_CREDENTIALS 123456
YDB_METADATA_CREDENTIALS 1
- authentification params, read more there: https://ydb.tech/en/docs/reference/ydb-sdk/auth

EventTimeColumnName - column for log time. Should be Timestamp type.
EventMetadataColumnName - column for log metadata. Should be Optional<String> type.
EventMessageColumnName - column for log message. Should be Optional<Json> type.

ParseToColumns - TODO, currenty doesn't work. Will parse log message keys to separate columns.
Example: {"key1": 123, "key2" : "value2"} - write 123 to column "key1" and "value2" to "key2"
```
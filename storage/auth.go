package storage

import (
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-yc"
)

func WithConfigCredentials(params map[string]string) ydb.Option {
	if serviceAccountKey, ok := params["YDB_SERVICE_ACCOUNT_KEY_CREDENTIALS"]; ok {
		return ydb.MergeOptions(
			yc.WithInternalCA(),
			yc.WithServiceAccountKeyCredentials(serviceAccountKey),
		)
	}
	if serviceAccountKeyFile, ok := params["YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"]; ok {
		return ydb.MergeOptions(
			yc.WithInternalCA(),
			yc.WithServiceAccountKeyFileCredentials(serviceAccountKeyFile),
		)
	}
	if params["YDB_ANONYMOUS_CREDENTIALS"] == "1" {
		return ydb.WithAnonymousCredentials()
	}
	if params["YDB_METADATA_CREDENTIALS"] == "1" {
		return ydb.MergeOptions(
			yc.WithInternalCA(),
			yc.WithMetadataCredentials(),
		)
	}
	if accessToken, ok := params["YDB_ACCESS_TOKEN_CREDENTIALS"]; ok {
		return ydb.WithAccessTokenCredentials(accessToken)
	}
	return ydb.MergeOptions(
		yc.WithInternalCA(),
		yc.WithMetadataCredentials(),
	)
}

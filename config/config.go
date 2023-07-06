package config

import (
	"errors"
	"strings"
	"unsafe"

	"github.com/fluent/fluent-bit-go/output"
)

type Config struct {
	ConnectionURL string
	TableName     string
	TablePath     string
	AuthParams    map[string]string
	WriteParams   WriteParams
}

type WriteParams struct {
	EventTimeColumnName     string
	EventMetadataColumnName string
	EventMessageColumnName  string
	ParseToColumns          bool
	EventCustomColumnNames  map[string]string
}

func ReadConfigFromPlugin(plugin unsafe.Pointer) (Config, error) {
	cfg := Config{AuthParams: make(map[string]string)}

	// Connection url.
	res := output.FLBPluginConfigKey(plugin, "ConnectionURL")
	if res == "" {
		return cfg, errors.New("not provided ConnectionURL for YDB")
	}

	cfg.ConnectionURL = res

	// Table name.
	res = output.FLBPluginConfigKey(plugin, "TableName")
	if res == "" {
		return cfg, errors.New("not provided TableName for YDB")
	}

	cfg.TableName = res

	// Table path.
	res = output.FLBPluginConfigKey(plugin, "TablePath")
	if res == "" {
		return cfg, errors.New("not provided TablePath for YDB")
	}

	cfg.TablePath = res

	// EventTimeColumnName.
	res = output.FLBPluginConfigKey(plugin, "EventTimeColumnName")
	if res == "" {
		return cfg, errors.New("not provided EventTimeColumnName for YDB")
	}

	cfg.WriteParams.EventTimeColumnName = res

	// EventMetadataColumnName.
	res = output.FLBPluginConfigKey(plugin, "EventMetadataColumnName")
	if res == "" {
		return cfg, errors.New("not provided EventMetadataColumnName for YDB")
	}

	cfg.WriteParams.EventMetadataColumnName = res

	// EventMessageColumnName.
	res = output.FLBPluginConfigKey(plugin, "EventMessageColumnName")
	if res == "" {
		return cfg, errors.New("not provided EventMessageColumnName for YDB")
	}

	cfg.WriteParams.EventMessageColumnName = res

	// Auth.
	res = output.FLBPluginConfigKey(plugin, "YDB_ANONYMOUS_CREDENTIALS")
	if res != "" {
		cfg.AuthParams["YDB_ANONYMOUS_CREDENTIALS"] = res
	}

	res = output.FLBPluginConfigKey(plugin, "YDB_SERVICE_ACCOUNT_KEY_CREDENTIALS")
	if res != "" {
		cfg.AuthParams["YDB_SERVICE_ACCOUNT_KEY_CREDENTIALS"] = res
	}

	res = output.FLBPluginConfigKey(plugin, "YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS")
	if res != "" {
		cfg.AuthParams["YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"] = res
	}

	res = output.FLBPluginConfigKey(plugin, "YDB_ACCESS_TOKEN_CREDENTIALS")
	if res != "" {
		cfg.AuthParams["YDB_ACCESS_TOKEN_CREDENTIALS"] = res
	}

	res = output.FLBPluginConfigKey(plugin, "YDB_METADATA_CREDENTIALS")
	if res != "" {
		cfg.AuthParams["YDB_METADATA_CREDENTIALS"] = res
	}

	res = output.FLBPluginConfigKey(plugin, "ParseToColumns")
	if strings.ToLower(res) == "true" {
		cfg.WriteParams.ParseToColumns = true
	}

	return cfg, nil
}

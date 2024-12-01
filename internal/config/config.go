package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"sort"
	"strings"
	"unsafe"

	"github.com/fluent/fluent-bit-go/output"
	"github.com/rs/zerolog"
	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
	yc "github.com/ydb-platform/ydb-go-yc"
)

const (
	ParamConnectionURL                  = "ConnectionURL"
	ParamCertificatesString             = "Certificates"
	ParamTablePath                      = "TablePath"
	ParamColumns                        = "Columns"
	ParamCredentialsYcServiceAccountKey = "CredentialsYcServiceAccountKey"
	ParamCredentialsYcMetadata          = "CredentialsYcMetadata"
	ParamCredentialsStatic              = "CredentialsStatic"
	ParamCredentialsToken               = "CredentialsToken"
	ParamCredentialsAnonymous           = "CredentialsAnonymous"
	ParamLogLevel                       = "LogLevel"

	KeyTimestamp = ".timestamp"
	KeyInput     = ".input"
	KeyOthers    = ".others"
	KeyHash      = ".hash"
)

type credentialsDescription struct {
	make  func(value string) (ydb.Option, error)
	about func() string
}

func isFile(path string) bool {
	_, err := os.Stat(path)

	return err == nil
}

var credentialsChooser = map[string]credentialsDescription{
	ParamCredentialsYcServiceAccountKey: {
		make: func(value string) (ydb.Option, error) {
			if isFile(value) {
				return yc.WithServiceAccountKeyFileCredentials(value), nil
			}

			return yc.WithServiceAccountKeyCredentials(value), nil
		},
		about: func() string {
			return fmt.Sprintf(
				"value of parameter '%s' must be a path to file or JSON",
				ParamCredentialsYcServiceAccountKey,
			)
		},
	},
	ParamCredentialsYcMetadata: {
		make: func(value string) (ydb.Option, error) {
			return yc.WithMetadataCredentials(), nil
		},
		about: func() string {
			return fmt.Sprintf(
				"used Yandex.Cloud metadata credentials if parameter '%s' is set to 1",
				ParamCredentialsYcMetadata,
			)
		},
	},
	ParamCredentialsStatic: {
		make: func(value string) (ydb.Option, error) {
			user, password, endpoint, err := parseParamCredentialsStaticValue(value)
			if err != nil {
				return nil, err
			}
			if endpoint == "" {
				return ydb.WithStaticCredentials(user, password), nil
			}

			return ydb.WithCredentials(credentials.NewStaticCredentials(user, password, endpoint)), nil
		},
		about: func() string {
			return fmt.Sprintf(
				"value of parameter '%s' must be a string with template 'user:password'",
				ParamCredentialsStatic,
			)
		},
	},
	ParamCredentialsToken: {
		make: func(value string) (ydb.Option, error) {
			return ydb.WithAccessTokenCredentials(value), nil
		},
		about: func() string {
			return fmt.Sprintf(
				"value of parameter '%s' must be a token",
				ParamCredentialsToken,
			)
		},
	},
	ParamCredentialsAnonymous: {
		make: func(value string) (ydb.Option, error) {
			return ydb.WithAnonymousCredentials(), nil
		},
		about: func() string {
			return fmt.Sprintf(
				"used anonymous credentials if parameter '%s' is set to 1",
				ParamCredentialsAnonymous,
			)
		},
	},
}

func parseParamCredentialsStaticValue(value string) (user, password, endpoint string, _ error) {
	if !strings.Contains(value, "://") {
		value = "blank://" + value
	}
	if !strings.Contains(value, "@") {
		value += "@endpoint"
		defer func() {
			endpoint = ""
		}()
	}
	u, err := url.Parse(value)
	if err != nil {
		return "", "", "", err
	}
	user = u.User.Username()
	password, _ = u.User.Password()
	endpoint = u.Host

	return
}

type Config struct {
	ConnectionURL     string
	Certificates      string
	CredentialsOption ydb.Option
	TablePath         string
	Columns           map[string]string
	LogLevel          zerolog.Level
}

func ydbCredentials(plugin unsafe.Pointer) (c ydb.Option, err error) {
	creds := make(map[string]ydb.Option, len(credentialsChooser))
	for paramName, description := range credentialsChooser {
		value := output.FLBPluginConfigKey(plugin, paramName)
		if value != "" {
			creds[paramName], err = description.make(value)
			if err != nil {
				return nil, fmt.Errorf("failed to create credentials: %w. %s", err, description.about())
			}
		}
	}
	switch len(creds) {
	case 0:
		return nil, fmt.Errorf("require one of credentials params: %v",
			func() (params []string) {
				for paramName := range credentialsChooser {
					params = append(params, paramName)
				}
				sort.Strings(params)

				return params
			}(),
		)
	case 1:
		for _, v := range creds {
			c = v

			break
		}

		return c, nil
	default:
		return nil, fmt.Errorf("require only one of credentials params: %v",
			func() (params []string) {
				for paramName := range creds {
					params = append(params, paramName)
				}
				sort.Strings(params)

				return params
			}(),
		)
	}
}

func ydbColumns(plugin unsafe.Pointer) (columns map[string]string, _ error) {
	columnsValue := output.FLBPluginConfigKey(plugin, ParamColumns)

	if isFile(columnsValue) {
		b, err := os.ReadFile(columnsValue)
		if err != nil {
			return nil, fmt.Errorf("failed to read file '%s': %w", columnsValue, err)
		}
		columnsValue = string(b)
	}

	err := json.Unmarshal([]byte(columnsValue), &columns)
	if err != nil {
		return nil, fmt.Errorf("failed to decode columns JSON: %w", err)
	}

	if _, has := columns[KeyTimestamp]; !has {
		return nil, fmt.Errorf("no required column '%s'", KeyTimestamp)
	}

	if _, has := columns[KeyInput]; !has {
		return nil, fmt.Errorf("no required column '%s'", KeyInput)
	}

	return columns, nil
}

func ReadConfigFromPlugin(plugin unsafe.Pointer) (cfg Config, _ error) {
	// Connection string
	connectionURL := output.FLBPluginConfigKey(plugin, ParamConnectionURL)
	if connectionURL == "" {
		return cfg, fmt.Errorf("not provided parameter '%s'", ParamConnectionURL)
	}
	cfg.ConnectionURL = connectionURL

	// Connection string
	certificates := output.FLBPluginConfigKey(plugin, ParamCertificatesString)
	if certificates != "" {
		cfg.Certificates = certificates
	}

	// Table path
	tablePath := output.FLBPluginConfigKey(plugin, ParamTablePath)
	if tablePath == "" {
		return cfg, fmt.Errorf("not provided parameter '%s'", ParamTablePath)
	}
	cfg.TablePath = tablePath

	// Table columns
	columns, err := ydbColumns(plugin)
	if err != nil {
		return cfg, fmt.Errorf("no columns: %w", err)
	}
	cfg.Columns = columns

	// credentials
	creds, err := ydbCredentials(plugin)
	if err != nil {
		return cfg, errors.New("required valid credentials")
	}
	cfg.CredentialsOption = creds

	// log level
	if lvl, err := zerolog.ParseLevel(output.FLBPluginConfigKey(plugin, ParamLogLevel)); err != nil {
		cfg.LogLevel = zerolog.InfoLevel
	} else {
		cfg.LogLevel = lvl
	}

	return cfg, nil
}

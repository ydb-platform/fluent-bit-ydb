package config

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"sort"
	"unsafe"

	"github.com/fluent/fluent-bit-go/output"
	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
	yc "github.com/ydb-platform/ydb-go-yc"

	"github.com/ydb-platform/fluent-bit-ydb/internal/model"
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

	KeyTimestamp = ".timestamp"
	KeyInput     = ".input"
)

type credentialsDescription struct {
	make  func(value string) (credentials.Credentials, error)
	about func() string
}

func isFile(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

var credentialsChooser = map[string]credentialsDescription{
	ParamCredentialsYcServiceAccountKey: {
		make: func(value string) (credentials.Credentials, error) {
			if isFile(value) {
				return yc.NewClient(yc.WithServiceFile(value))
			}
			return yc.NewClient(yc.WithServiceKey(value))
		},
		about: func() string {
			return fmt.Sprintf(
				"value of parameter '%s' must be a path to file or JSON",
				ParamCredentialsYcServiceAccountKey,
			)
		},
	},
	ParamCredentialsYcMetadata: {
		make: func(value string) (credentials.Credentials, error) {
			return yc.NewInstanceServiceAccount(), nil
		},
		about: func() string {
			return fmt.Sprintf(
				"used Yandex.Cloud metadata credentials if parameter '%s' is set to 1",
				ParamCredentialsYcMetadata,
			)
		},
	},
	ParamCredentialsStatic: {
		make: func(value string) (credentials.Credentials, error) {
			user, password, endpoint, err := parseParamCredentialsStaticValue(value)
			if err != nil {
				return nil, err
			}
			return credentials.NewStaticCredentials(user, password, endpoint), nil
		},
		about: func() string {
			return fmt.Sprintf(
				"value of parameter '%s' must be a string with template 'user:password@auth_endpoint:port'",
				ParamCredentialsStatic,
			)
		},
	},
	ParamCredentialsToken: {
		make: func(value string) (credentials.Credentials, error) {
			return credentials.NewAccessTokenCredentials(value), nil
		},
		about: func() string {
			return fmt.Sprintf(
				"value of parameter '%s' must be a token",
				ParamCredentialsToken,
			)
		},
	},
	ParamCredentialsAnonymous: {
		make: func(value string) (credentials.Credentials, error) {
			return credentials.NewAnonymousCredentials(), nil
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
	u, err := url.Parse("blank://" + value)
	if err != nil {
		return "", "", "", err
	}
	user = u.User.Username()
	password, _ = u.User.Password()
	endpoint = u.Host
	return
}

type Config struct {
	ConnectionURL string
	Certificates  string
	Credentials   credentials.Credentials
	TablePath     string
	Columns       map[string]model.Column
}

func ydbCredentials(plugin unsafe.Pointer) (c credentials.Credentials, err error) {
	creds := make(map[string]credentials.Credentials, len(credentialsChooser))
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

func ydbColumns(plugin unsafe.Pointer) (columns map[string]model.Column, _ error) {
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
		return cfg, fmt.Errorf("required valid credentials")
	}
	cfg.Credentials = creds

	return cfg, nil
}

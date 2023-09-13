package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"reflect"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"

	"github.com/ydb-platform/fluent-bit-ydb/internal/config"
	"github.com/ydb-platform/fluent-bit-ydb/internal/log"
	"github.com/ydb-platform/fluent-bit-ydb/internal/model"
)

var (
	_ interface {
		Write(event []*model.Event) error
	} = (*YDB)(nil)

	_ interface {
		Exit() error
	} = (*YDB)(nil)
)

type YDB struct {
	db           *ydb.Driver
	cfg          *config.Config
	fieldMapping map[string]options.Column // {fieldName : Column}
}

func New(cfg *config.Config) (*YDB, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	opts := []ydb.Option{ydb.WithCredentials(cfg.Credentials)}
	if cfg.Certificates != "" {
		_, err := os.Stat(cfg.Certificates)
		if err == nil {
			opts = append(opts, ydb.WithCertificatesFromFile(cfg.Certificates))
		} else {
			opts = append(opts, ydb.WithCertificatesFromPem([]byte(cfg.Certificates)))
		}
	}

	// Opening connection.
	db, err := ydb.Open(ctx, cfg.ConnectionURL, opts...)
	if err != nil {
		return nil, err
	}

	s := &YDB{
		db:  db,
		cfg: cfg,
	}

	fieldMapping, err := s.resolveFieldMapping(ctx)
	if err != nil {
		return s, err
	}
	s.fieldMapping = fieldMapping

	return s, nil
}

const (
	textType      = "Text"
	bytesType     = "Bytes"
	jsonType      = "Json"
	timestampType = "Timestamp"
)

func (s *YDB) resolveFieldMapping(ctx context.Context) (map[string]options.Column, error) {
	var (
		columns map[string]options.Column
	)

	// Getting table columns names and types.
	if err := s.db.Table().Do(ctx,
		func(ctx context.Context, session table.Session) (err error) {
			desc, err := session.DescribeTable(ctx, path.Join(s.db.Name(), s.cfg.TablePath))
			if err != nil {
				return fmt.Errorf("failed to describe table `%s`: %w", path.Join(s.db.Name(), s.cfg.TablePath), err)
			}

			columns = make(map[string]options.Column, len(desc.Columns))

			for i := range desc.Columns {
				columns[desc.Columns[i].Name] = desc.Columns[i]
			}

			return nil
		},
	); err != nil {
		return nil, fmt.Errorf("failed to check columns names and types: %w", err)
	}

	// Define log fields to columns mapping.
	fieldToColumnMapping := make(map[string]options.Column, len(s.cfg.Columns))

	for field, column := range columns {
		_, has := columns[column.Name]
		if !has {
			return nil, fmt.Errorf("not found column '%s' in destination table for field %s", column.Name, field)
		}
		fieldToColumnMapping[field] = columns[column.Name]
	}

	return fieldToColumnMapping, nil
}

func type2Type(t types.Type, v interface{}) (types.Value, error) {
	optional, columnType := convertTypeIfOptional(t)
	columnTypeYql := yqlType(columnType)

	switch v := v.(type) {
	case time.Time:
		switch columnTypeYql {
		case timestampType:
			return convertValueIfOptional(optional, types.TimestampValueFromTime(v)), nil
		default:
			return nil, fmt.Errorf("not supported conversion (time) from '%s' to '%s' (%s)", v, columnTypeYql, t)
		}
	case []byte:
		switch columnTypeYql {
		case bytesType:
			return convertValueIfOptional(optional, types.BytesValue(v)), nil
		case textType:
			return convertValueIfOptional(optional, types.TextValue(string(v))), nil
		default:
			return nil, fmt.Errorf("not supported conversion (bytes) from '%s' to '%s' (%s)", v, columnTypeYql, t)
		}
	case string:
		switch columnTypeYql {
		case bytesType:
			return convertValueIfOptional(optional, types.BytesValueFromString(v)), nil
		case textType:
			return convertValueIfOptional(optional, types.TextValue(v)), nil
		default:
			return nil, fmt.Errorf("not supported conversion (string) from '%s' to '%s' (%s)", v, columnTypeYql, t)
		}
	case map[interface{}]interface{}:
		j, err := json.Marshal(convertByteFieldsToString(v))
		if err != nil {
			return nil, fmt.Errorf("failed to marshal json value: %w. Value: %#v", err, v)
		}

		switch columnTypeYql {
		case bytesType:
			return convertValueIfOptional(optional, types.BytesValue(j)), nil
		case textType:
			return convertValueIfOptional(optional, types.TextValue(string(j))), nil
		case jsonType:
			return convertValueIfOptional(optional, types.JSONValueFromBytes(j)), nil
		default:
			return nil, fmt.Errorf("not supported conversion (map) '%s' to '%s' (%s)", v, columnTypeYql, t)
		}
	default:
		return nil, fmt.Errorf("not supported source type '%s', type: %s", v, reflect.TypeOf(v))
	}
}

func (s *YDB) Write(events []*model.Event) error {
	rows := make([]types.Value, 0, len(events))

	for _, event := range events {
		columns := make([]types.StructValueOption, 0, len(event.Message)+2)

		v, err := type2Type(s.fieldMapping[config.KeyTimestamp].Type, event.Timestamp)
		if err != nil {
			return err
		}
		columns = append(columns, types.StructFieldValue(s.fieldMapping[config.KeyTimestamp].Name, v))

		v, err = type2Type(s.fieldMapping[config.KeyInput].Type, event.Metadata)
		if err != nil {
			return err
		}
		columns = append(columns, types.StructFieldValue(s.fieldMapping[config.KeyInput].Name, v))

		for field, value := range event.Message {
			column, exists := s.fieldMapping[field]
			if !exists {
				log.Warn(fmt.Sprintf("column for message key: %s (value: %s) not found, skip", field, value))
				continue
			}

			v, err := type2Type(column.Type, value)
			if err != nil {
				return err
			}
			columns = append(columns, types.StructFieldValue(s.fieldMapping[field].Name, v))
		}

		rows = append(rows, types.StructValue(columns...))
	}

	return s.db.Table().Do(context.Background(), func(ctx context.Context, sess table.Session) error {
		return sess.BulkUpsert(ctx, path.Join(s.db.Name(), s.cfg.TablePath), types.ListValue(rows...))
	})
}

func (s *YDB) Exit() error {
	return s.db.Close(context.Background())
}

func yqlType(t types.Type) string {
	switch s := t.Yql(); s {
	case "Utf8":
		return textType
	case "String":
		return bytesType
	default:
		return s
	}
}

func convertByteFieldsToString(in map[interface{}]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(in))

	for key, value := range in {
		key := key.(string)

		switch value := value.(type) {
		case map[interface{}]interface{}:
			out[key] = convertByteFieldsToString(value)
		case []byte:
			out[key] = string(value)
		default:
			out[key] = value
		}
	}

	return out
}

func convertTypeIfOptional(t types.Type) (bool, types.Type) {
	optional, inner := types.IsOptional(t)
	if optional {
		return optional, inner
	}
	return false, t
}

func convertValueIfOptional(optional bool, v types.Value) types.Value {
	if optional {
		return types.OptionalValue(v)
	}
	return v
}

func pointer[T any](v T) *T {
	return &v
}

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

	ydb := &YDB{
		db:  db,
		cfg: cfg,
	}

	var columns map[string]options.Column

	// Getting table columns names and types.
	if err = db.Table().Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			desc, err := s.DescribeTable(ctx, path.Join(db.Name(), cfg.TablePath))
			if err != nil {
				return fmt.Errorf("failed to describe table `%s`: %w", path.Join(db.Name(), cfg.TablePath), err)
			}

			columns = make(map[string]options.Column, len(desc.Columns))

			for _, column := range desc.Columns {
				columns[column.Name] = column
			}

			return nil
		},
	); err != nil {
		return nil, fmt.Errorf("failed to check columns names and types: %w", err)
	}

	// Define log fields to columns mapping.
	mapping, err := ydbFieldMapping(columns, cfg.Columns)
	if err != nil {
		return ydb, err
	}
	ydb.fieldMapping = mapping

	return ydb, nil
}

const (
	textType      = "Text"
	bytesType     = "Bytes"
	jsonType      = "Json"
	timestampType = "Timestamp"
)

func type2Type(c options.Column, v interface{}) (types.Value, error) {
	optional, columnType := columnTypeIfOptional(c)
	columnTypeYql := yqlType(columnType)

	switch v := v.(type) {
	case time.Time:
		switch columnTypeYql {
		case timestampType:
			return convertIfColumnOptional(optional, types.TimestampValueFromTime(v)), nil
		default:
			return nil, fmt.Errorf("not supported conversion (time) from '%s' to '%s'", v, c.Type)
		}
	case []byte:
		switch columnTypeYql {
		case bytesType:
			return convertIfColumnOptional(optional, types.BytesValue(v)), nil
		case textType:
			return convertIfColumnOptional(optional, types.TextValue(string(v))), nil
		default:
			return nil, fmt.Errorf("not supported conversion (bytes) from '%s' to '%s'", v, c.Type)
		}
	case string:
		switch columnTypeYql {
		case bytesType:
			return convertIfColumnOptional(optional, types.BytesValueFromString(v)), nil
		case textType:
			return convertIfColumnOptional(optional, types.TextValue(v)), nil
		default:
			return nil, fmt.Errorf("not supported conversion (string) from '%s' to '%s'", v, c.Type)
		}
	case map[interface{}]interface{}:
		j, err := json.Marshal(convertByteFieldsToString(v))
		if err != nil {
			return nil, fmt.Errorf("failed to marshal json value: %w. Value: %#v", err, v)
		}

		switch columnTypeYql {
		case bytesType:
			return convertIfColumnOptional(optional, types.BytesValue(j)), nil
		case textType:
			return convertIfColumnOptional(optional, types.TextValue(string(j))), nil
		case jsonType:
			return convertIfColumnOptional(optional, types.JSONValueFromBytes(j)), nil
		default:
			return nil, fmt.Errorf("not supported conversion (map) '%s' to '%s'", v, c.Type)
		}
	default:
		return nil, fmt.Errorf("not supported source type '%s', type: %s", v, reflect.TypeOf(v))
	}
}

func (s *YDB) Write(events []*model.Event) error {
	rows := make([]types.Value, 0, len(events))

	for _, event := range events {
		columns := make([]types.StructValueOption, 0, len(event.Message)+2)

		v, err := type2Type(s.fieldMapping[config.KeyTimestamp], event.Timestamp)
		if err != nil {
			return err
		}
		columns = append(columns, types.StructFieldValue(s.fieldMapping[config.KeyTimestamp].Name, v))

		v, err = type2Type(s.fieldMapping[config.KeyInput], event.Metadata)
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

			v, err := type2Type(column, value)
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

func ydbFieldMapping(columns map[string]options.Column, columnMapping map[string]model.Column) (map[string]options.Column, error) {
	fieldToColumnMapping := make(map[string]options.Column, len(columnMapping))

	for field, column := range columnMapping {
		_, has := columns[column.Name]
		if !has {
			return nil, fmt.Errorf("not found column '%s' in destination table for field %s", column.Name, field)
		}
		fieldToColumnMapping[field] = columns[column.Name]
	}

	return fieldToColumnMapping, nil
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

func columnTypeIfOptional(c options.Column) (bool, types.Type) {
	optional, innerType := types.IsOptional(c.Type)
	if optional {
		return optional, innerType
	}
	return false, c.Type
}

func convertIfColumnOptional(optional bool, v types.Value) types.Value {
	if optional {
		return types.OptionalValue(v)
	}
	return v
}

func pointer[T any](v T) *T {
	return &v
}

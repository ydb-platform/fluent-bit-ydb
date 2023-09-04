package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"reflect"
	"time"

	"github.com/ydb-platform/fluent-bit-ydb/internal/log"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"

	"github.com/ydb-platform/fluent-bit-ydb/internal/config"
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
	tableColumns map[string]types.Type // {columnName : type}.
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

	// Getting table columns names and types.
	if err = db.Table().Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			desc, err := s.DescribeTable(ctx, path.Join(db.Name(), cfg.TablePath))
			if err != nil {
				return fmt.Errorf("failed to describe table `%s`: %w", path.Join(db.Name(), cfg.TablePath), err)
			}

			ydb.tableColumns = make(map[string]types.Type, len(desc.Columns))

			for i := range desc.Columns {
				ydb.tableColumns[desc.Columns[i].Name] = desc.Columns[i].Type
			}

			return nil
		},
	); err != nil {
		return nil, fmt.Errorf("failed to check columns names and types: %w", err)
	}

	// Checking for valid table for writing.
	err = validateColumns(ydb.tableColumns, cfg.Columns)
	if err != nil {
		return ydb, err
	}

	return ydb, nil
}

const (
	textType      = "Text"
	bytesType     = "Bytes"
	jsonType      = "Json"
	timestampType = "Timestamp"
)

func type2Type(toType string, v interface{}) (types.Value, error) {
	switch v := v.(type) {
	case time.Time:
		switch toType {
		case timestampType:
			return types.TimestampValueFromTime(v), nil
		default:
			return nil, fmt.Errorf("not supported conversion (time) from '%s' to '%s'", v, toType)
		}
	case []byte:
		switch toType {
		case bytesType:
			return types.BytesValue(v), nil
		case textType:
			return types.TextValue(string(v)), nil
		default:
			return nil, fmt.Errorf("not supported conversion (bytes) from '%s' to '%s'", v, toType)
		}
	case string:
		switch toType {
		case bytesType:
			return types.BytesValueFromString(v), nil
		case textType:
			return types.TextValue(v), nil
		default:
			return nil, fmt.Errorf("not supported conversion (string) from '%s' to '%s'", v, toType)
		}
	case map[interface{}]interface{}:
		j, err := json.Marshal(convert(v))
		if err != nil {
			return nil, fmt.Errorf("failed to marshal json value: %+v. Value: %#v", err, v)
		}

		switch toType {
		case bytesType:
			return types.BytesValue(j), nil
		case textType:
			return types.TextValue(string(j)), nil
		case jsonType:
			return types.JSONValue(string(j)), nil
		default:
			return nil, fmt.Errorf("not supported conversion (map) '%s' to '%s'", v, toType)
		}
	default:
		return nil, fmt.Errorf("not supported source type '%s', type: %s", v, reflect.TypeOf(v))
	}
}

func (s *YDB) Write(events []*model.Event) error {
	rows := make([]types.Value, 0, len(events))

	for _, event := range events {
		columns := make([]types.StructValueOption, 0, len(event.Message)+2)

		v, err := type2Type(s.cfg.Columns[config.KeyTimestamp].Type, event.Timestamp)
		if err != nil {
			return err
		}
		columns = append(columns, types.StructFieldValue(s.cfg.Columns[config.KeyTimestamp].Name, v))

		v, err = type2Type(s.cfg.Columns[config.KeyInput].Type, event.Metadata)
		if err != nil {
			return err
		}
		columns = append(columns, types.StructFieldValue(s.cfg.Columns[config.KeyInput].Name, v))

		for k, value := range event.Message {
			column, exists := s.cfg.Columns[k]
			if !exists {
				log.Warn(fmt.Sprintf("column for message key: %s (value: %s) not found, skip", k, value))
				continue
			}

			v, err := type2Type(column.Type, value)
			if err != nil {
				return err
			}
			columns = append(columns, types.StructFieldValue(s.cfg.Columns[k].Name, v))
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

func validateColumns(columns map[string]types.Type, mapping map[string]model.Column) error {
	for key := range mapping {
		t, has := columns[mapping[key].Name]
		if !has {
			return fmt.Errorf("not found column '%s' in destination table", mapping[key].Name)
		}
		if mapping[key].Type != yqlType(t) {
			return fmt.Errorf("wrong type of column '%s': '%s' (expected '%s')",
				mapping[key].Name,
				yqlType(t),
				mapping[key].Type,
			)
		}
	}
	return nil
}

func convert(in map[interface{}]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(in))

	for key, value := range in {
		key := key.(string)

		switch value := value.(type) {
		case map[interface{}]interface{}:
			out[key] = convert(value)
		case []byte:
			out[key] = string(value)
		default:
			out[key] = value
		}
	}

	return out
}

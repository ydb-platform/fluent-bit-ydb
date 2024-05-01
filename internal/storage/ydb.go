package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"reflect"
	"strings"
	"time"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
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
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second) //nolint:gomnd
	defer cancel()

	opts := []ydb.Option{cfg.CredentialsOption}
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

	if err := s.resolveFieldMapping(ctx); err != nil {
		return s, err
	}

	return s, nil
}

const (
	textType         = "Text"
	bytesType        = "Bytes"
	jsonType         = "Json"
	jsonDocumentType = "JsonDocument"
	timestampType    = "Timestamp"
)

func (s *YDB) resolveFieldMapping(ctx context.Context) error {
	var columns map[string]options.Column

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
		return fmt.Errorf("failed to check columns names and types: %w", err)
	}

	// Define log fields to columns mapping.
	fieldToColumnMapping := make(map[string]options.Column, len(s.cfg.Columns))

	for field, column := range s.cfg.Columns {
		_, has := columns[column]
		if !has {
			return fmt.Errorf("not found column '%s' in destination table for field %s", column, field)
		}
		fieldToColumnMapping[field] = columns[column]
	}

	s.fieldMapping = fieldToColumnMapping

	return nil
}

const (
	Sz8   = 8
	Sz16  = 16
	Sz32  = 32
	Sz64  = 64
	Sz30M = 30 * 1024 * 1024
)

func type2Type(t types.Type, v interface{}) (types.Value, int, error) {
	optional, columnType := convertTypeIfOptional(t)
	columnTypeYql := yqlType(columnType)

	if v == nil {
		if optional {
			switch columnTypeYql {
			case timestampType:
				return types.NullableTimestampValue(nil), Sz64, nil
			case bytesType:
				return types.NullableBytesValue(nil), Sz16, nil
			case textType:
				return types.NullableTextValue(nil), Sz16, nil
			case jsonType:
				return types.NullableJSONValue(nil), Sz16, nil
			case jsonDocumentType:
				return types.NullableJSONDocumentValue(nil), Sz16, nil
			}
		} else {
			switch columnTypeYql {
			case timestampType:
				return types.TimestampValueFromTime(time.UnixMicro(0)), Sz64, nil
			case bytesType:
				return types.BytesValue(make([]byte, 0)), Sz16, nil
			case textType:
				return types.TextValue(""), Sz16, nil
			case jsonType:
				return types.JSONValue("{}"), Sz32, nil
			case jsonDocumentType:
				return types.JSONDocumentValue("{}"), Sz32, nil
			}
		}

		return nil, -1, fmt.Errorf("not supported conversion from NULL to '%s' (%s)", columnTypeYql, t)
	}

	switch v := v.(type) {
	case time.Time:
		switch columnTypeYql {
		case timestampType:
			return convertValueIfOptional(optional, types.TimestampValueFromTime(v)), Sz32, nil
		default:
			return nil, -1, fmt.Errorf("not supported conversion (time) from '%s' to '%s' (%s)", v, columnTypeYql, t)
		}
	case []byte:
		switch columnTypeYql {
		case bytesType:
			return convertValueIfOptional(optional, types.BytesValue(v)), Sz8 + len(v), nil
		case textType:
			return convertValueIfOptional(optional, types.TextValue(string(v))), Sz8 + len(v), nil
		case timestampType:
			return convertTimestamp(optional, string(v)), Sz64, nil
		default:
			return nil, -1, fmt.Errorf("not supported conversion (bytes) from '%s' to '%s' (%s)", v, columnTypeYql, t)
		}
	case string:
		switch columnTypeYql {
		case bytesType:
			return convertValueIfOptional(optional, types.BytesValueFromString(v)), Sz8 + len(v), nil
		case textType:
			return convertValueIfOptional(optional, types.TextValue(v)), Sz8 + len(v), nil
		case timestampType:
			return convertTimestamp(optional, v), Sz64, nil
		default:
			return nil, -1, fmt.Errorf("not supported conversion (string) from '%s' to '%s' (%s)", v, columnTypeYql, t)
		}
	case map[interface{}]interface{}:
		j, err := json.Marshal(convertByteFieldsToString(v))
		if err != nil {
			return nil, -1, fmt.Errorf("failed to marshal json value: %w. Value: %#v", err, v)
		}

		switch columnTypeYql {
		case bytesType:
			return convertValueIfOptional(optional, types.BytesValue(j)), Sz8 + len(j), nil
		case textType:
			return convertValueIfOptional(optional, types.TextValue(string(j))), Sz8 + len(j), nil
		case jsonType:
			return convertValueIfOptional(optional, types.JSONValue(string(j))), Sz8 + len(j), nil
		case jsonDocumentType:
			return convertValueIfOptional(optional, types.JSONDocumentValue(string(j))), Sz8 + len(j), nil
		case timestampType:
			return convertTimestamp(optional, string(j)), Sz64, nil
		default:
			return nil, -1, fmt.Errorf("not supported conversion (map) '%s' to '%s' (%s)", v, columnTypeYql, t)
		}
	default:
		return nil, -1, fmt.Errorf("not supported source type '%s', type: %s", v, reflect.TypeOf(v))
	}
}

func (s *YDB) BuildColumnUsageMap() map[string]bool {
	m := make(map[string]bool)
	for k := range s.fieldMapping {
		if !strings.HasPrefix(k, ".") {
			m[k] = true
		}
	}

	return m
}

func (s *YDB) ConvertRows(events []*model.Event) ([]types.Value, int, error) {
	rows := make([]types.Value, 0, len(events))
	maxrowbytes := 1
	colCount := len(s.fieldMapping)

	var othersValue map[interface{}]interface{}
	othersColumn, othersUsed := s.fieldMapping[config.KeyOthers]

	for _, event := range events {
		if othersUsed {
			othersValue = make(map[interface{}]interface{})
		}
		rowbytes := Sz64 + Sz64
		columns := make([]types.StructValueOption, 0, colCount)

		v, vlen, err := type2Type(s.fieldMapping[config.KeyTimestamp].Type, event.Timestamp)
		if err != nil {
			return nil, -1, err
		}
		columns = append(columns, types.StructFieldValue(s.fieldMapping[config.KeyTimestamp].Name, v))
		rowbytes += Sz64 + vlen
		rowbytes += len(s.fieldMapping[config.KeyTimestamp].Name)

		v, vlen, err = type2Type(s.fieldMapping[config.KeyInput].Type, event.Metadata)
		if err != nil {
			return nil, -1, err
		}
		columns = append(columns, types.StructFieldValue(s.fieldMapping[config.KeyInput].Name, v))
		rowbytes += Sz64 + vlen
		rowbytes += len(s.fieldMapping[config.KeyInput].Name)

		columnUsageMap := s.BuildColumnUsageMap()

		for field, value := range event.Message {
			column, exists := s.fieldMapping[field]
			if !exists {
				if othersUsed {
					othersValue[field] = value
				} else {
					log.Debug(fmt.Sprintf("column for message key: %s (value: %v) not found, skipped", field, value))
				}

				continue
			}

			v, vlen, err = type2Type(column.Type, value)
			if err != nil {
				log.Warn(fmt.Sprintf("failed to convert column for message key: %s (value: %v), skipped. %v",
					field, value, err))

				continue
			}
			columns = append(columns, types.StructFieldValue(s.fieldMapping[field].Name, v))
			rowbytes += Sz64 + vlen
			rowbytes += len(s.fieldMapping[field].Name)
			delete(columnUsageMap, field)
		}

		if len(columnUsageMap) > 0 {
			// some columns were not included
			for cname := range columnUsageMap {
				cdef := s.fieldMapping[cname]
				v, vlen, err = type2Type(cdef.Type, nil)
				if err != nil {
					return nil, -1, err
				}
				columns = append(columns, types.StructFieldValue(cdef.Name, v))
				rowbytes += Sz64 + vlen
				rowbytes += len(cdef.Name)
			}
		}

		if othersUsed {
			v, vlen, err := type2Type(othersColumn.Type, othersValue)
			if err != nil {
				return nil, -1, err
			}
			columns = append(columns, types.StructFieldValue(othersColumn.Name, v))
			rowbytes += Sz64 + vlen
			rowbytes += len(othersColumn.Name)
		}

		rows = append(rows, types.StructValue(columns...))
		if rowbytes > maxrowbytes {
			maxrowbytes = rowbytes
		}
	}

	return rows, maxrowbytes, nil
}

func (s *YDB) Write(events []*model.Event) error {
	// convert the input events to the database rows
	rows, maxrowbytes, err := s.ConvertRows(events)
	if err != nil {
		return err
	}
	sz := len(events)
	// split the rows into portions having size of no more than 30 megabytes
	portion := Sz30M / maxrowbytes
	if portion < 1 {
		portion = 1
	}
	if portion > sz {
		portion = sz
	}
	log.Debug(fmt.Sprintf("Got events block of size %d with portion %d and %d max bytes per row...",
		sz, portion, maxrowbytes))
	position := 0
	for position < sz {
		finish := position + portion
		if finish > sz {
			finish = sz
		}
		part := rows[position:finish]
		log.Debug(fmt.Sprintf("...Processing positions [%d:%d], size %d", position, finish, len(part)))
		err = s.db.Table().Do(context.Background(),
			func(ctx context.Context, sess table.Session) error {
				return sess.BulkUpsert(ctx, path.Join(s.db.Name(), s.cfg.TablePath), types.ListValue(part...))
			},
		)
		if err != nil {
			log.Debug(fmt.Sprintf("...BulkUpsert failed: %v", err))

			break
		}
		log.Debug("...BulkUpsert succeeded")
		position = finish
	}

	if ydb.IsOperationErrorSchemeError(err) {
		log.Warn("Detected scheme error, trying to resolve field mapping from table description")
		resolveErr := s.resolveFieldMapping(context.Background())
		if resolveErr != nil {
			return errors.Join(err, resolveErr)
		}
	}

	return err
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

const (
	LenTimestamp3339 = 24
)

func convertTimestamp(optional bool, v string) types.Value {
	var err error
	if len(v) == LenTimestamp3339 {
		var tv time.Time
		tv, err = time.Parse(time.RFC3339, v)
		if err == nil {
			return convertValueIfOptional(optional, types.TimestampValueFromTime(tv))
		}
	}
	if err == nil {
		log.Warn(fmt.Sprintf("failed to parse value [%s] as timestamp - unknown format", v))
	} else {
		log.Warn(fmt.Sprintf("failed to parse value [%s] as timestamp - %s", v, err))
	}
	if optional {
		return types.NullValue(types.TypeTimestamp)
	}

	return convertValueIfOptional(optional, types.TimestampValueFromTime(time.Now()))
}

func pointer[T any](v T) *T {
	return &v
}

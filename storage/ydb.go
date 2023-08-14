package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"path"
	"time"

	"github.com/ydb-platform/fluent-bit-ydb/config"
	"github.com/ydb-platform/fluent-bit-ydb/model"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

type YDB struct {
	db           *ydb.Driver
	cfg          config.Config
	tableColumns map[string]types.Type // {columnName : type}.
}

func NewYDB(cfg config.Config) (*YDB, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Opening connection.
	db, err := ydb.Open(ctx, cfg.ConnectionURL, WithConfigCredentials(cfg.AuthParams))
	if err != nil {
		log.Printf("[ydb] Failed open YDB: %v", err)
		return nil, err
	}

	ydb := &YDB{
		db:  db,
		cfg: cfg,
	}

	// Getting table columns names and types.
	err = db.Table().Do(ctx,
		func(ctx context.Context, s table.Session) error {
			desc, err := s.DescribeTable(ctx, path.Join(db.Name(), cfg.TableName))
			if err != nil {
				fmt.Printf("%+v\n", err)
				return err
			}

			columns := make(map[string]types.Type, len(desc.Columns))

			for _, column := range desc.Columns {
				columns[column.Name] = column.Type
			}

			ydb.tableColumns = columns

			return nil
		},
	)
	if err != nil {
		log.Printf("[ydb] Failed check columns names and types: %v", err)
		return nil, err
	}

	// Checking for valid table for writing.
	err = validateColumns(ydb.tableColumns, cfg.WriteParams)
	if err != nil {
		return ydb, err
	}

	return ydb, nil
}

func (s *YDB) Write(events []*model.Event) error {
	rows := make([]types.Value, 0, len(events))

	for _, event := range events {
		b, err := json.Marshal(event.Message)
		if err != nil {
			log.Printf("Failed marshal event %v: %v", event.Message, b)
			continue
		}

		rows = append(rows, types.StructValue(
			types.StructFieldValue(s.cfg.WriteParams.EventTimeColumnName, types.TimestampValueFromTime(event.Timestamp)),
			types.StructFieldValue(s.cfg.WriteParams.EventMetadataColumnName, types.StringValueFromString(event.Metadata)),
			types.StructFieldValue(s.cfg.WriteParams.EventMessageColumnName, types.JSONValueFromBytes(b)),
		))
	}

	err := s.db.Table().Do(context.Background(), func(ctx context.Context, sess table.Session) error {
		return sess.BulkUpsert(ctx, s.cfg.TablePath, types.ListValue(rows...))
	})

	return err
}

func (s *YDB) Exit() error {
	return s.db.Close(context.Background())
}

func validateColumns(columns map[string]types.Type, params config.WriteParams) error {
	foundGoodTimeColumn := false
	foundGoodMetadataColumn := false
	foundGoodMessageColumn := false

	for columnName, columnType := range columns {
		if !foundGoodTimeColumn && params.EventTimeColumnName == columnName {
			if columnType == types.TypeTimestamp {
				foundGoodTimeColumn = true
			} else {
				return fmt.Errorf("Provided column name %s for timestamp. Excepted Timestamp/Uint64 type for this column, but got %v\n",
					params.EventTimeColumnName, columnType)
			}
		}

		if !foundGoodMetadataColumn && params.EventMetadataColumnName == columnName {
			if columnType == types.Optional(types.TypeString) {
				foundGoodMetadataColumn = true
			} else {
				return fmt.Errorf("Provided column name %s for metadata. Excepted String type for this column, but got %v\n",
					params.EventMetadataColumnName, columnType)
			}
		}

		if !foundGoodMessageColumn && params.EventMessageColumnName == columnName {
			if columnType == types.Optional(types.TypeJSON) {
				foundGoodMessageColumn = true
			} else {
				return fmt.Errorf("Provided column name %s for message. Excepted Json/JsonDocument type for this column, but got %v\n",
					params.EventMessageColumnName, columnType)
			}
		}
	}

	if !foundGoodTimeColumn {
		return fmt.Errorf("Failed find %s column for writing event time.\n", params.EventTimeColumnName)
	}

	if !foundGoodMetadataColumn {
		return fmt.Errorf("Failed find %s column for writing event metadata.\n", params.EventMetadataColumnName)
	}

	if !foundGoodMessageColumn {
		return fmt.Errorf("Failed find %s column for writing event message.\n", params.EventMessageColumnName)
	}

	return nil
}

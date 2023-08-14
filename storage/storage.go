package storage

import (
	"github.com/ydb-platform/fluent-bit-ydb/config"
	"github.com/ydb-platform/fluent-bit-ydb/model"
)

type Storager interface {
	Write(event []*model.Event) error
	Exit() error
}

func New(cfg config.Config) (Storager, error) {
	return NewYDB(cfg)
}

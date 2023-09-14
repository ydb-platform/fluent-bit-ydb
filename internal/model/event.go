package model

import (
	"time"
)

type Event struct {
	Timestamp time.Time
	Metadata  string
	Message   map[string]interface{}
}

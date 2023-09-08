package model

import (
	"strings"
)

type Column struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

func (c Column) IsOptional() bool {
	return strings.HasPrefix(c.Type, "Optional<") && strings.HasSuffix(c.Type, ">")
}

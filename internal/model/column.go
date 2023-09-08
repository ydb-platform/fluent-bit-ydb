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

func (c Column) GetType() string {
	if c.IsOptional() {
		return strings.Replace(strings.Replace(c.Type, "Optional<", "", -1), ">", "", -1)
	}
	return c.Type
}

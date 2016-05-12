package bitdb

import (
	"reflect"
)

const (
	col = "col"
	warp = "warp"
)

type field struct  {
	column string
	kind reflect.Kind
	warp string
	value interface{}
	elemType reflect.Type
}

type DataBaseTable interface {
	TableName() string
	PkColumn() string
	DeleteColumn() string
}



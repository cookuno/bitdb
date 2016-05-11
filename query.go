package bitdb

import (
	"errors"
	"fmt"
	"reflect"
	"database/sql"
	"time"
	"encoding/json"
)

func (self *BitDB) Find(ql string, targetSlice interface{}, args ...interface{}) error {
	if ql == "" {
		return errors.New("ql is empty")
	}
	if targetSlice == nil {
		return errors.New("targets is nil")
	}
	sliceValue := reflect.Indirect(reflect.ValueOf(targetSlice))
	if sliceValue.Kind() != reflect.Slice && sliceValue.Kind() != reflect.Map {
		return errors.New("needs a pointer to a slice or a map")
	}
	sliceElementType := sliceValue.Type().Elem()
	fields := make(map[string]reflect.Type)
	for i := 0; i < sliceElementType.NumField(); i++ {
		tag := sliceElementType.Field(i).Tag.Get("column")
		fieldType := sliceElementType.Field(i).Type
		fields[tag] = fieldType
	}
	db := self.db
	stmt, stmtErr := db.Prepare(ql)
	if stmtErr != nil {
		return stmtErr
	}
	defer stmt.Close()
	rows, rowErr := stmt.Query(args...)
	if rowErr != nil {
		return rowErr
	}
	defer rows.Close()
	columns, columnsErr := rows.Columns()
	if columnsErr != nil {
		return columnsErr
	}
	columnLen := len(columns)
	vals := make([]interface{},0, columnLen)
	for i := 0 ; i < columnLen ; i ++  {
		column := columns[i]
		if t, ok := fields[column] ; ok {
			if t.Kind() == reflect.String {
				var cell sql.NullString
				vals = append(vals, &cell)
			} else if t.Kind() == reflect.Int || t.Kind() == reflect.Int16 || t.Kind() == reflect.Int32 || t.Kind() == reflect.Int64 {
				var cell sql.NullInt64
				vals = append(vals, &cell)
			} else if t.Kind() == reflect.Float32 || t.Kind() == reflect.Float64 {
				var cell sql.NullFloat64
				vals = append(vals, &cell)
			} else if t.Kind() == reflect.Bool {
				var cell sql.NullBool
				vals = append(vals, &cell)
			} else if t.Kind() == reflect.TypeOf(time.Time{}).Kind() {
				var cell time.Time
				vals = append(vals, &cell)
			} else if t.Kind() == reflect.TypeOf(json.RawMessage{}).Kind() {
				var cell json.RawMessage
				vals = append(vals, &cell)
			} else if t.Kind() == reflect.TypeOf([]byte{}).Kind() {
				var cell []byte
				vals = append(vals, &cell)
			} else {
				panic(fmt.Sprint("unknow type", column, t.Kind()))
			}
		} else {
			var cell interface{}
			vals = append(vals, &cell)
		}
	}
	for rows.Next() {
		scanErr := rows.Scan(vals...)
		if scanErr != nil {
			return scanErr
		}
		for i := 0; i < len(vals); i++ {
			column := columns[i]
			val := vals[i]
			if t, ok := fields[column] ; ok {
				if t.Kind() == reflect.String {
					fmt.Print((val).(*sql.NullString).String, " ")
				} else if t.Kind() == reflect.Int || t.Kind() == reflect.Int16 || t.Kind() == reflect.Int32 || t.Kind() == reflect.Int64 {
					fmt.Print((val).(*sql.NullInt64).Int64, " ")
				} else if t.Kind() == reflect.Float32 || t.Kind() == reflect.Float64 {
					fmt.Print((val).(*sql.NullFloat64).Float64, " ")
				} else if t.Kind() == reflect.Bool {
					fmt.Print((val).(*sql.NullBool).Bool, " ")
				} else if t.Kind() == reflect.TypeOf(time.Time{}).Kind() {
					fmt.Print((val).(*time.Time), " ")
				} else if t.Kind() == reflect.TypeOf(json.RawMessage{}).Kind() {
					fmt.Print((val).(*json.RawMessage), " ")
				} else if t.Kind() == reflect.TypeOf([]byte{}).Kind() {
					fmt.Print((val).(*[]byte), " ")
				} else {
					panic(fmt.Sprint("unknow type", column, t.Kind()))
				}
			}
			// TODO BUNLD TARGETS
		}
		fmt.Println("")
	}
	return nil
}

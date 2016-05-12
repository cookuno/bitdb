package bitdb

import (
	"errors"
	"reflect"
	"database/sql"
	"time"
	"encoding/json"
	"strings"
	"fmt"
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
	fieldMap := make(map[string]field)
	for i := 0; i < sliceElementType.NumField(); i++ {
		colTag := strings.TrimSpace(sliceElementType.Field(i).Tag.Get(col))
		warpTag := strings.TrimSpace(sliceElementType.Field(i).Tag.Get(warp))
		fieldKind := sliceElementType.Field(i).Type.Kind()
		f := field{
			column:colTag,
			kind:fieldKind,
			warp:warpTag,
			elemType:sliceElementType.Field(i).Type,
		}
		fieldMap[colTag] = f
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
	scans := make([]interface{},0, columnLen)
	for i := 0 ; i < columnLen ; i ++  {
		column := columns[i]
		if t, ok := fieldMap[column] ; ok {
			if strings.TrimSpace(t.warp) == "json" && t.kind == reflect.Struct{
				var cell []byte
				scans = append(scans, &cell)
			} else {
				if t.kind == reflect.String {
					var cell sql.NullString
					scans = append(scans, &cell)
				} else if t.kind == reflect.Int64 {
					var cell sql.NullInt64
					scans = append(scans, &cell)
				} else if t.kind == reflect.Float64 {
					var cell sql.NullFloat64
					scans = append(scans, &cell)
				} else if t.kind == reflect.Bool {
					var cell sql.NullBool
					scans = append(scans, &cell)
				} else if t.kind == reflect.TypeOf(time.Time{}).Kind() {
					var cell time.Time
					scans = append(scans, &cell)
				} else if t.kind == reflect.TypeOf([]byte{}).Kind() {
					var cell []byte
					scans = append(scans, &cell)
				} else {
					panic(fmt.Sprint("unknow type", column, t))
				}
			}
		} else {
			var cell interface{}
			scans = append(scans, &cell)
		}
	}
	for rows.Next() {
		scanErr := rows.Scan(scans...)
		if scanErr != nil {
			return scanErr
		}
		sliceElement := reflect.New(sliceElementType).Interface()
		fieldValueMap := make(map[string]field)
		for i := 0; i < len(scans); i++ {
			column := columns[i]
			val := scans[i]
			if t, ok := fieldMap[column] ; ok {
				if strings.TrimSpace(t.warp) == "json" && t.kind == reflect.Struct{
					v := reflect.ValueOf(val).Elem()
					if v.Kind() == reflect.String {
						str := (val).(string)
						t.value = []byte(str)
					}
					bytes := reflect.Indirect(reflect.ValueOf(val)).Bytes()
					jsonVal := reflect.New(t.elemType).Interface()
					unMarshalErr := json.Unmarshal(bytes, &jsonVal)
					if unMarshalErr != nil {
						panic(unMarshalErr)
					}
					t.value = jsonVal
				} else {
					if t.kind == reflect.String {
						t.value = (val).(*sql.NullString).String
					} else if t.kind == reflect.Int64 {
						t.value = (val).(*sql.NullInt64).Int64
					} else if t.kind == reflect.Float64 {
						t.value = (val).(*sql.NullFloat64).Float64
					} else if t.kind == reflect.Bool {
						t.value = (val).(*sql.NullBool).Bool
					} else if t.kind == reflect.TypeOf(time.Time{}).Kind() {
						t.value = val
					} else if t.kind == reflect.TypeOf([]byte{}).Kind() {
						t.value = (val).(*[]byte)
					} else {
						panic(fmt.Sprint("unknow type", column, t))
					}
				}
				fieldValueMap[t.column] = t
			}
			rowElementMap(sliceElement, fieldValueMap)
		}
		sliceValue.Set(reflect.Append(sliceValue, reflect.Indirect(reflect.ValueOf(sliceElement))))
	}
	return nil
}

func rowElementMap(elem interface{}, fieldMap map[string]field)  {
	v := reflect.ValueOf(elem).Elem()
	t := reflect.TypeOf(elem).Elem()
	for i := 0 ; i < t.NumField() ; i ++  {
		f := v.Field(i)
		colTag := t.Field(i).Tag.Get(col)
		if field, ok := fieldMap[colTag]; ok {
			f.Set(reflect.Indirect(reflect.ValueOf(field.value)))
		}
	}
}

package database

import (
	"database/sql"
	"time"
	"fmt"
)

// Results are returned in the form of a map for easy return via a web api
type (
	Result  map[string]interface{}
	Results []Result
)

// Column represents a column from a database, it has a name and a value
type Column struct {
	Name string
	Val  interface{}
}

// Implement Scanner for Column
func (c *Column) Scan(src interface{}) error {
	switch x := src.(type) {
	case int64:
		c.Val = x
	case float64:
		c.Val = x
	case bool:
		c.Val = x
	case string:
		c.Val = x
	case []byte:
		c.Val = x
	case time.Time:
		c.Val = x
	case nil:
		c.Val = x
	default:
		return fmt.Errorf("Scan was given an object of unexpected type %T", src)
	}
	return nil
}

// Create a bunch of empty columns with the row names
func emptyColumns(names []string) []Column {
	columns := make([]Column, len(names))
	for idx, name := range names {
		columns[idx].Name = name
	}
	return columns
}

// Given an array of columns return an array of blank interfaces to satisfy `rows.Scan`
func toInterfaces(cols []Column) []interface{} {
	interfaces := make([]interface{}, len(cols))
	for i := 0; i < len(cols); i++ {
		interfaces[i] = &cols[i]
	}
	return interfaces
}

// Given an slice of columns (name and value pairs) return a map from name to value
func makeResult(columns []Column) Result {
	result := make(Result, len(columns))
	for _, c := range columns {
		result[c.Name] = c.Val
	}
	return result
}

// Given *sql.Rows return a slice of maps from name to value
func rowsToResults(rows *sql.Rows) (results Results, err error) {
	names, err := rows.Columns()
	if err != nil {
		return
	}

	for rows.Next() {
		cols := emptyColumns(names)
		rows.Scan(toInterfaces(cols)...)
		results = append(results, makeResult(cols))
	}
	err = rows.Err()
	if err != nil {
		results = nil
	}
	return
}
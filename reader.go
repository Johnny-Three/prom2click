package main

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	tag "github.com/prom2click/label"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/remote"
	"time"
	"reflect"
	"bytes"
	"github.com/prom2click/job"
)

type p2cReader struct {
	conf *config
	db   *sql.DB
	jm   *job.JobManager
}

// getTimePeriod return select and where SQL chunks relating to the time period -or- error
func (r *p2cReader) getTimePeriod(query *remote.Query) (string, string, error) {

	var tselSQL = "SELECT COUNT() AS CNT, (intDiv(toUInt32(ts), %d) * %d) * 1000 as t"
	var twhereSQL = "WHERE date >= toDate(%d) AND ts >= toDateTime(%d) AND ts <= toDateTime(%d)"
	var err error
	tstart := query.StartTimestampMs / 1000
	tend := query.EndTimestampMs / 1000

	// valid time period
	if tend < tstart {
		err = errors.New("Start time is after end time")
		return "", "", err
	}

	// need time period in seconds
	tperiod := tend - tstart

	// need to split time period into <nsamples> - also, don't divide by zero
	if r.conf.CHMaxSamples < 1 {
		err = fmt.Errorf(fmt.Sprintf("Invalid CHMaxSamples: %d", r.conf.CHMaxSamples))
		return "", "", err
	}
	taggr := tperiod / int64(r.conf.CHMaxSamples)
	if taggr < int64(r.conf.CHMinPeriod) {
		taggr = int64(r.conf.CHMinPeriod)
	}

	selectSQL := fmt.Sprintf(tselSQL, taggr, taggr)
	whereSQL := fmt.Sprintf(twhereSQL, tstart, tstart, tend)

	return selectSQL, whereSQL, nil
}

//make the sql body ..
func (r *p2cReader) getSQLOut(matchers []*remote.LabelMatcher) (sqlhead, sqlbody string) {

	// join 为函数地址
	f := func(m *remote.LabelMatcher, name string) (sqlr string) {

		//tag.Namespace
		switch m.Type {
		case remote.MatchType_EQUAL:
			sqlr = fmt.Sprintf(` %s='%s' `, name, strings.Replace(m.Value, `'`, `\'`, -1))
		case remote.MatchType_NOT_EQUAL:
			sqlr = fmt.Sprintf(` %s!='%s' `, name, strings.Replace(m.Value, `'`, `\'`, -1))
		case remote.MatchType_REGEX_MATCH:
			sqlr = fmt.Sprintf(` match(%s, %s) = 1 `, name, strings.Replace(m.Value, `/`, `\/`, -1))
		case remote.MatchType_REGEX_NO_MATCH:
			sqlr = fmt.Sprintf(` match(%s, %s) = 0 `, name, strings.Replace(m.Value, `/`, `\/`, -1))
		}
		return
	}

	mslicebody := []string{}
	mslicehead := []string{}
	for _, m := range matchers {

		switch(m.Name) {
		case tag.Namespace, tag.Keyspace, tag.Ip, tag.Shard, tag.App, tag.Component, tag.Container, tag.Job, model.MetricNameLabel:
			{
				if m.Name == model.MetricNameLabel {
					m.Name = "name"
				}
				if m.Name == tag.Container {
					m.Name = "containername"
				}
				mslicebody = append(mslicebody, f(m, m.Name))
				mslicehead = append(mslicehead, m.Name)
			}
		default:
			continue
		}
	}

	return strings.Join(mslicehead, ","), strings.Join(mslicebody, "and")
}

func (r *p2cReader) getSQL(query *remote.Query) (string, error) {
	// time related select sql, where sql chunks
	tselectSQL, twhereSQL, err := r.getTimePeriod(query)
	if err != nil {
		return "", err
	}
	head, body := r.getSQLOut(query.Matchers)
	// put select and where together with group by etc
	tempSQL := "%s,%s, quantile(%f)(val) as value FROM %s.%s %s and %s GROUP BY t,%s ORDER BY t asc"
	sql := fmt.Sprintf(tempSQL, tselectSQL, head, r.conf.CHQuantile, r.conf.ChDB, r.conf.ChTable, twhereSQL, body, head)
	return sql, nil
}

func NewP2CReader(conf *config, jm *job.JobManager) (*p2cReader, error) {
	var err error
	r := new(p2cReader)
	r.conf = conf
	r.jm = jm
	r.db, err = sql.Open("clickhouse", r.conf.ChDSN)
	r.db.SetMaxOpenConns(100)
	r.db.SetMaxIdleConns(10)
	r.db.Ping()
	if err != nil {
		fmt.Printf("Error connecting to clickhouse: %s\n", err.Error())
		return r, err
	}

	return r, nil
}

func (r *p2cReader) Read(req *remote.ReadRequest) (*remote.ReadResponse, error) {
	var err error
	var sqlStr string
	var rows *sql.Rows

	resp := remote.ReadResponse{
		Results: []*remote.QueryResult{
			{Timeseries: make([]*remote.TimeSeries, 0, 0)},
		},
	}
	// need to map tags to timeseries to record samples
	var tsres = make(map[string]*remote.TimeSeries)

	// for debugging/figuring out query format/etc
	rcount := 0

	for _, q := range req.Queries {

		tm1 := time.Unix(q.StartTimestampMs/1000, 0)
		tm2 := time.Unix(q.EndTimestampMs/1000, 0)
		// remove me..
		fmt.Printf("\nquery: start: %s, end: %s\n\n", tm1.Format("2006-01-02 03:04:05 PM"), tm2.Format("2006-01-02 03:04:05 PM"))
		fmt.Printf("\nsql comes from prometheus %s\n", q.String())

		// get the select sql
		sqlStr, err = r.getSQL(q)
		fmt.Printf("query: running sql: %s\n\n", sqlStr)
		if err != nil {
			fmt.Printf("Error: reader: getSQL: %s\n", err.Error())
			return &resp, err
		}

		// get the select sql
		if err != nil {
			fmt.Printf("Error: reader: getSQL: %s\n", err.Error())
			return &resp, err
		}

		// todo: metrics on number of errors, rows, selects, timings, etc
		rows, err = r.db.Query(sqlStr)
		cols, _ := rows.Columns()
		if err != nil {
			fmt.Printf("Error: query failed: %s", sqlStr)
			fmt.Printf("Error: query error: %s\n", err)
			return &resp, err
		}
		// build map of timeseries from sql result
		for rows.Next() {
			rcount++
			// Create a slice of interface{}'s to represent each column,
			// and a second slice to contain pointers to each item in the columns slice.
			columns := make([]interface{}, len(cols))
			columnPointers := make([]interface{}, len(cols))
			for i, _ := range columns {
				columnPointers[i] = &columns[i]
			}
			if err = rows.Scan(columnPointers...); err != nil {
				fmt.Printf("Error: scan: %s\n", err.Error())
			}

			// Create our map, and retrieve the value for each column from the pointers slice,
			// storing it in the map with the name of the column as the key.
			m := make(map[string]interface{})
			for i, colName := range cols {
				val := columnPointers[i].(*interface{})
				m[colName] = *val
			}

			// Outputs: map[columnName:value columnName2:value2 columnName3:value3 ...]
			//fmt.Print(m)
			key, value, t, labels := makeLabels(m)
			// borrowed from influx remote storage adapter - array sep
			//key := strings.Join(tags, "\xff")
			ts, ok := tsres[key]
			if !ok {
				ts = &remote.TimeSeries{
					Labels: labels,
				}
				tsres[key] = ts
			}
			ts.Samples = append(ts.Samples, &remote.Sample{
				Value:       float64(value),
				TimestampMs: int64(t),
			})
		}
	}
	rows.Close()
	fmt.Printf("query:: returning %d rows for %d queries\n", rcount, len(req.Queries))
	return &resp, nil

}

//TODO://根据新的map值反射出名字和值...
func makeLabels(tags map[string]interface{}) (key string, val float64, ts uint64, lpairs []*remote.LabelPair) {
	lpairs = make([]*remote.LabelPair, 0, len(tags))
	// (currently) writer includes __name__ in tags so no need to add it here
	// may change this to save space later..
	var keyarray []string
	var str bytes.Buffer
	for colname, tag := range tags {

		//获取interface的类型
		//t := reflect.TypeOf(tag)
		if colname == "value" {
			val = reflect.ValueOf(tag).Interface().(float64)
			continue
		}

		if colname == "t" {
			ts = reflect.ValueOf(tag).Interface().(uint64)
			continue
		}

		if colname == "CNT" {
			continue
		}

		cv := reflect.ValueOf(tag).Interface().(string)
		if cv == "" {
			continue
		}

		lpairs = append(lpairs, &remote.LabelPair{
			Name:  colname,
			Value: cv,
		})
		str.WriteString(colname)
		str.WriteString(cv)
		keyarray = append(keyarray, str.String())
	}
	return strings.Join(keyarray, "\xff"), val, ts, lpairs
}

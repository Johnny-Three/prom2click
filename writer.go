package main

import (
	"database/sql"
	"fmt"
	"github.com/kshvakov/clickhouse"
	"sync"
	"github.com/prometheus/client_golang/prometheus"
	pro "github.com/prom2click/protocal"
)

var insertSQL = `INSERT INTO %s.%s
	(ip,app,name,job,namespace,shard,keyspace,component,containername, val, ts,date,tags)
	VALUES	(?, ?, ?, ?, ?, ?,?,?,?,?,?,?,?)`

type p2cWriter struct {
	conf     *config
	requests chan *pro.K8sRequest
	wg       sync.WaitGroup
	db       *sql.DB
	table    string
	tx       prometheus.Counter
	ko       prometheus.Counter
	test     prometheus.Counter
	timings  prometheus.Histogram
}

func NewP2CWriter(conf *config, job string, table string, reqs chan *pro.K8sRequest) (*p2cWriter, error) {
	var err error
	w := new(p2cWriter)
	w.conf = conf
	w.requests = reqs
	w.table = table
	w.db, err = sql.Open("clickhouse", w.conf.ChDSN)
	w.db.SetMaxOpenConns(20)
	w.db.SetMaxIdleConns(2)
	w.db.Ping()
	if err != nil {
		fmt.Printf("Error connecting to clickhouse: %s\n", err.Error())
		return w, err
	}

	return w, nil
}

func (w *p2cWriter) Start() {

	go func() {
		fmt.Println("Writer starting..")
		sql := fmt.Sprintf(insertSQL, w.conf.ChDB, w.table)
		ok := true
		for {
			// get next batch of requests
			var reqs []*pro.K8sRequest

			//tstart := time.Now()
			for i := 0; i < w.conf.ChBatch; i++ {
				var req *pro.K8sRequest
				// get requet and also check if channel is closed
				req, ok = <-w.requests
				if !ok {
					fmt.Println("Writer stopping..")
					break
				}
				reqs = append(reqs, req)
			}

			// ensure we have something to send..
			nmetrics := len(reqs)
			if nmetrics < 1 {
				continue
			}

			// post them to db all at once
			tx, err := w.db.Begin()
			if err != nil {
				fmt.Printf("Error: begin transaction: %s\n", err.Error())
				continue
			}

			// build statements
			smt, err := tx.Prepare(sql)
			if err != nil {
				fmt.Printf("Error: prepare statement: %s\n", err.Error())
			}
			for _, req := range reqs {
				if err != nil {
					fmt.Printf("Error: prepare statement: %s\n", err.Error())
					continue
				}
				_, err = smt.Exec(req.Ip, req.App, req.Name, req.Job, req.Namespace, req.Shard, req.Keyspace, req.Component, req.Containername,
					req.Val, req.Ts, req.Ts, clickhouse.Array(req.Tags))

				if err != nil {
					fmt.Printf("Error: statement exec: %s\n", err.Error())
				}
			}

			// commit and record metrics
			if err = tx.Commit(); err != nil {
				fmt.Printf("Error: commit failed: %s\n", err.Error())
			} else {

			}
			//MUST close fd here ! it is must must must or we will encounter too many open files error !
			smt.Close()
		}
		fmt.Println("Writer stopped..")
	}()
}

func (w *p2cWriter) Wait() {
	w.wg.Wait()
}

package main

import (
	"database/sql"
	"fmt"
	"time"
	"github.com/kshvakov/clickhouse"
	"sync"
	"github.com/prometheus/client_golang/prometheus"
)

/*
ip String DEFAULT 'default',
app String DEFAULT 'default',
name String DEFAULT 'default',
job String DEFAULT 'default',
namespace String DEFAULT 'default',
shard String DEFAULT 'default',
keyspace String DEFAULT 'default',
component String DEFAULT 'default',
containername String DEFAULT 'default',
*/

var insertSQL = `INSERT INTO %s.%s
	(ip,app,name,job,namespace,shard,keyspace,component,containername, val, ts,date,tags)
	VALUES	(?, ?, ?, ?, ?, ?,?,?,?,?,?,?,?)`

type p2cWriter struct {
	conf     *config
	requests chan *p2cRequest
	wg       sync.WaitGroup
	db       *sql.DB
	tx       prometheus.Counter
	ko       prometheus.Counter
	test     prometheus.Counter
	timings  prometheus.Histogram
}

func NewP2CWriter(conf *config, reqs chan *p2cRequest) (*p2cWriter, error) {
	var err error
	w := new(p2cWriter)
	w.conf = conf
	w.requests = reqs
	w.db, err = sql.Open("clickhouse", w.conf.ChDSN)
	w.db.SetMaxOpenConns(100)
	w.db.SetMaxIdleConns(10)
	w.db.Ping()
	if err != nil {
		fmt.Printf("Error connecting to clickhouse: %s\n", err.Error())
		return w, err
	}

	w.tx = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "sent_samples_total",
			Help: "Total number of processed samples sent to remote storage.",
		},
	)

	w.ko = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "failed_samples_total",
			Help: "Total number of processed samples which failed on send to remote storage.",
		},
	)

	w.test = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "prometheus_remote_storage_sent_batch_duration_seconds_bucket_test",
			Help: "Test metric to ensure backfilled metrics are readable via prometheus.",
		},
	)

	w.timings = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "sent_batch_duration_seconds",
			Help:    "Duration of sample batch send calls to the remote storage.",
			Buckets: prometheus.DefBuckets,
		},
	)
	prometheus.MustRegister(w.tx)
	prometheus.MustRegister(w.ko)
	prometheus.MustRegister(w.test)
	prometheus.MustRegister(w.timings)

	return w, nil
}

func (w *p2cWriter) Start() {

	go func() {
		w.wg.Add(1)
		fmt.Println("Writer starting..")
		sql := fmt.Sprintf(insertSQL, w.conf.ChDB, w.conf.ChTable)
		ok := true
		for ok {
			w.test.Add(1)
			// get next batch of requests
			var reqs []*p2cRequest

			tstart := time.Now()
			for i := 0; i < w.conf.ChBatch; i++ {
				var req *p2cRequest
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
				w.ko.Add(1.0)
				continue
			}

			// build statements
			smt, err := tx.Prepare(sql)
			if err != nil {
				fmt.Printf("Error: prepare statement: %s\n", err.Error())
			}
			defer smt.Close() // danger!
			for _, req := range reqs {
				if err != nil {
					fmt.Printf("Error: prepare statement: %s\n", err.Error())
					w.ko.Add(1.0)
					continue
				}
				//	(ip,app,name,job,namespace,shard,keyspace,component,containername, val, ts)
				_, err = smt.Exec(req.ip, req.app, req.name, req.job, req.namespace, req.shard, req.keyspace, req.component, req.containername,
					req.val, req.ts, req.ts, clickhouse.Array(req.tags))

				if err != nil {
					fmt.Printf("Error: statement exec: %s\n", err.Error())
					w.ko.Add(1.0)
				}
			}

			// commit and record metrics
			if err = tx.Commit(); err != nil {
				fmt.Printf("Error: commit failed: %s\n", err.Error())
				w.ko.Add(1.0)
			} else {
				w.tx.Add(float64(nmetrics))
				w.timings.Observe(float64(time.Since(tstart)))
			}

		}
		fmt.Println("Writer stopped..")
		w.wg.Done()
	}()
}

func (w *p2cWriter) Wait() {
	w.wg.Wait()
}

package main

import (
	"io/ioutil"
	"net/http"
	"time"

	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/remote"
	"gopkg.in/tylerb/graceful.v1"
	tag "github.com/prom2click/label"
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
val Float64,
ts DateTime,
date Date DEFAULT toDate(0),
reserved1 String DEFAULT 'default',
reserved2 String DEFAULT 'default',
reserved3 String DEFAULT 'default',
reserved4 String DEFAULT 'default',
*/

type p2cRequest struct {
	ip            string
	app           string
	name          string
	job           string
	namespace     string
	shard         string
	keyspace      string
	component     string
	containername string
	val           float64
	ts            time.Time
	tags          []string
}

type p2cServer struct {
	requests chan *p2cRequest
	mux      *http.ServeMux
	conf     *config
	writer   *p2cWriter
	reader   *p2cReader
	rx       prometheus.Counter
}

func Newp2cRequest() (*p2cRequest) {
	p2cr := &p2cRequest{
		ip:            "x",
		app:           "x",
		name:          "x",
		job:           "x",
		namespace:     "x",
		shard:         "x",
		keyspace:      "x",
		component:     "x",
		containername: "x",
		val:           0.0,
		ts:            time.Now(),
		tags:          []string{},
	}
	return p2cr
}

func NewP2CServer(conf *config) (*p2cServer, error) {
	var err error
	c := new(p2cServer)
	c.requests = make(chan *p2cRequest, conf.ChanSize)
	c.mux = http.NewServeMux()
	c.conf = conf

	c.writer, err = NewP2CWriter(conf, c.requests)
	if err != nil {
		fmt.Printf("Error creating clickhouse writer: %s\n", err.Error())
		return c, err
	}

	c.reader, err = NewP2CReader(conf)
	if err != nil {
		fmt.Printf("Error creating clickhouse reader: %s\n", err.Error())
		return c, err
	}

	c.rx = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "received_samples_total",
			Help: "Total number of received samples.",
		},
	)
	prometheus.MustRegister(c.rx)

	c.mux.HandleFunc(c.conf.HTTPWritePath, func(w http.ResponseWriter, r *http.Request) {
		//close the body ..
		defer r.Body.Close()

		compressed, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		reqBuf, err := snappy.Decode(nil, compressed)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var req remote.WriteRequest
		if err := proto.Unmarshal(reqBuf, &req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		c.process(req)
	})

	c.mux.HandleFunc("/read", func(w http.ResponseWriter, r *http.Request) {
		//close the body ..
		defer r.Body.Close()

		compressed, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		reqBuf, err := snappy.Decode(nil, compressed)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var req remote.ReadRequest
		if err := proto.Unmarshal(reqBuf, &req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		start := time.Now()
		fmt.Printf("the query stars at the time %v\n", start)
		var resp *remote.ReadResponse
		resp, err = c.reader.Read(&req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		end := time.Now().Sub(start)

		data, err := proto.Marshal(resp)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/x-protobuf")
		w.Header().Set("Content-Encoding", "snappy")

		compressed = snappy.Encode(nil, data)
		if _, err := w.Write(compressed); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		fmt.Printf("the query exaushted %v\n", end)

	})

	c.mux.Handle(c.conf.HTTPMetricsPath, prometheus.InstrumentHandler(
		c.conf.HTTPMetricsPath, prometheus.UninstrumentedHandler(),
	))

	return c, nil
}

func (c *p2cServer) process(req remote.WriteRequest) {
	for _, series := range req.Timeseries {
		c.rx.Add(float64(len(series.Samples)))
		p2c := Newp2cRequest()
		for _, label := range series.Labels {
			if model.LabelName(label.Name) == model.MetricNameLabel {
				p2c.name = label.Value
			}
			if model.LabelName(label.Name) == model.JobLabel {
				p2c.job = label.Value
			}
			if model.LabelName(label.Name) == tag.Namespace {
				p2c.namespace = label.Value
			}
			if model.LabelName(label.Name) == tag.Ip {
				p2c.ip = label.Value
			}
			if model.LabelName(label.Name) == tag.App {
				p2c.app = label.Value
			}
			if model.LabelName(label.Name) == tag.Shard {
				p2c.shard = label.Value
			}
			if model.LabelName(label.Name) == tag.Keyspace {
				p2c.keyspace = label.Value
			}
			if model.LabelName(label.Name) == tag.Component {
				p2c.component = label.Value
			}
			if model.LabelName(label.Name) == tag.Container {
				p2c.containername = label.Value
			}

			t := fmt.Sprintf("%s=%s", label.Name, label.Value)
			p2c.tags = append(p2c.tags, t)
		}
		for _, sample := range series.Samples {
			p2c.ts = time.Unix(sample.TimestampMs/1000, 0)
			p2c.val = sample.Value
			c.requests <- p2c
		}
	}
}

func (c *p2cServer) Start() error {
	fmt.Println("HTTP server starting...")
	c.writer.Start()
	return graceful.RunWithErr(c.conf.HTTPAddr, c.conf.HTTPTimeout, c.mux)
}

func (c *p2cServer) Shutdown() {
	close(c.requests)
	c.writer.Wait()

	wchan := make(chan struct{})
	go func() {
		c.writer.Wait()
		close(wchan)
	}()

	select {
	case <-wchan:
		fmt.Println("Writer shutdown cleanly..")
		// All done!
	case <-time.After(10 * time.Second):
		fmt.Println("Writer shutdown timed out, samples will be lost..")
	}

}

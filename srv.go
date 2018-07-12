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
	pro "github.com/prom2click/protocal"
	"github.com/prom2click/job"
)

type p2cServer struct {
	requests chan *pro.K8sRequest
	mux      *http.ServeMux
	conf     *config
	writers  []*p2cWriter
	reader   *p2cReader
	jm       *job.JobManager
	rx       prometheus.Counter
}

func NewP2CServer(conf *config) (*p2cServer, error) {
	var err error
	c := new(p2cServer)
	c.requests = make(chan *pro.K8sRequest, conf.ChanSize)
	c.mux = http.NewServeMux()
	c.conf = conf

	//Initial JobManager ..
	jm, err := job.NewJobManager(c.conf.ChBatch)
	if err != nil {
		return nil, err
	}
	c.jm = jm
	//根据不同的job生成不同的writer，每个writer都有自己监控的channel，channel中的值由server分发
	for jobname, channel := range c.jm.GetJobs() {
		table := c.jm.GetTableAccordingJobName(jobname)
		if table == "" {
			fmt.Println("Error find table according jobname in configfile")
			continue
		}
		writer, err := NewP2CWriter(conf, jobname, table, channel)
		if err != nil {
			fmt.Printf("Error creating clickhouse writer: %s\n", err.Error())
		}
		writer.Start()
		c.writers = append(c.writers, writer)
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
		p2c := pro.NewK8sRequest()
		for _, label := range series.Labels {
			if model.LabelName(label.Name) == model.MetricNameLabel {
				p2c.Name = label.Value
			}
			if model.LabelName(label.Name) == model.JobLabel {
				p2c.Job = label.Value
			}
			if model.LabelName(label.Name) == tag.Namespace {
				p2c.Namespace = label.Value
			}
			if model.LabelName(label.Name) == tag.Ip {
				p2c.Ip = label.Value
			}
			if model.LabelName(label.Name) == tag.App {
				p2c.App = label.Value
			}
			if model.LabelName(label.Name) == tag.Shard {
				p2c.Shard = label.Value
			}
			if model.LabelName(label.Name) == tag.Keyspace {
				p2c.Keyspace = label.Value
			}
			if model.LabelName(label.Name) == tag.Component {
				p2c.Component = label.Value
			}
			if model.LabelName(label.Name) == tag.Container {
				p2c.Containername = label.Value
			}

			t := fmt.Sprintf("%s=%s", label.Name, label.Value)
			p2c.Tags = append(p2c.Tags, t)
		}
		for _, sample := range series.Samples {
			p2c.Ts = time.Unix(sample.TimestampMs/1000, 0)
			p2c.Val = sample.Value
			if channel, err := c.jm.GetChannelAccordingJobname(p2c.Job); err == nil {
				channel <- p2c
			}
		}
	}
}

func (c *p2cServer) Start() error {
	fmt.Println("HTTP server starting...")
	return graceful.RunWithErr(c.conf.HTTPAddr, c.conf.HTTPTimeout, c.mux)
}

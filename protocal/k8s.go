package protocal

import "time"

type K8sRequest struct {
	Ip            string
	App           string
	Name          string
	Job           string
	Namespace     string
	Shard         string
	Keyspace      string
	Component     string
	Containername string
	Val           float64
	Ts            time.Time
	Tags          []string
}


func NewK8sRequest() (*K8sRequest) {
	p2cr := &K8sRequest{
		Ip:            "x",
		App:           "x",
		Name:          "x",
		Job:           "x",
		Namespace:     "x",
		Shard:         "x",
		Keyspace:      "x",
		Component:     "x",
		Containername: "x",
		Val:           0.0,
		Ts:            time.Now(),
		Tags:          []string{},
	}
	return p2cr
}

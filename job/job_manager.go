package job

import (
	"github.com/prom2click/config"
	pro "github.com/prom2click/protocal"
	"fmt"
)

type JobManager struct {
	jobs map[string]chan *pro.K8sRequest
	cfm  *config.ConfigManager
}

//新建jobmanager，新建jobmanager读取配置文件，目前不支持指定配置文件路径，配置文件改变会影响到configmanager中保存的jobmap
//新建的jobmanager根据job数量建立job  channel ,从而数据写入解析到job后进入到不同的channel中，写入到不同的表中。
//如果job名字不匹配，那么，数据就会被过滤出去
func NewJobManager(capacity int) (jm *JobManager, err error) {

	config := config.NewConfigManager()
	err = config.Load()
	if err != nil {
		return nil, err
	}

	jm = &JobManager{
		jobs: nil,
		cfm:  config,
	}

	tmp := jm.cfm.GetJobMap()
	jm.jobs = make(map[string]chan *pro.K8sRequest, len(tmp))
	for jobname, _ := range tmp {
		jm.jobs[jobname] = make(chan *pro.K8sRequest, capacity)
	}

	return jm, nil
}

//返回一个只写channel
func (jm *JobManager) GetChannelAccordingJobname(jobname string) (chan<- *pro.K8sRequest, error) {
	val, ok := jm.jobs[jobname]
	if ok {
		return val, nil
	} else {
		return nil, fmt.Errorf("job not found")
	}
}

func (jm *JobManager) GetTableAccordingJobName(jobname string) string {
	jobmap := jm.cfm.GetJobMap()
	table, ok := jobmap[jobname]
	if ok {
		return table
	} else {
		return ""
	}
	return ""
}

func (jm *JobManager) GetJobs() map[string]chan *pro.K8sRequest {
	return jm.jobs
}

package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

var (
	mu sync.Mutex
)

type Coordinator struct {
	// Your definitions here.
	files            []string      // 传入的文件数组
	ReducerNum       int           // 传入的参数决定需要多少个reducer
	JobId            int           // 用于生成Job的特殊id
	DistPhase        Phase         // 目前整个框架应该处于什么任务阶段
	reduceJobChannel chan *Job     // 使用chan保证并发安全
	mapJobChannel    chan *Job     // 使用chan保证并发安全
	JobMetaHolder    JobMetaHolder // 存着Job

}

// JobMetaInfo 保存任务的元数据
type JobMetaInfo struct {
	state     State     // 任务的状态
	StartTime time.Time // 任务的开始时间
	JobAdr    *Job      // 传入任务的指针
}

// JobMetaHolder 保存全部任务的元数据
type JobMetaHolder struct {
	MetaMap map[int]*JobMetaInfo
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:            files,
		ReducerNum:       nReduce,
		DistPhase:        MapPhase,
		mapJobChannel:    make(chan *Job, len(files)),
		reduceJobChannel: make(chan *Job, nReduce),
		JobMetaHolder: JobMetaHolder{
			MetaMap: make(map[int]*JobMetaInfo, len(files)+nReduce), // 任务的总数应该是files + Reducer的数量
		},
	}
	c.makemapJobs(files)

	c.server()

	go c.CrashDetector()

	return &c
}

func (c *Coordinator) CrashDetector() {
	for {
		time.Sleep(time.Second * 2)
		mu.Lock()
		if c.DistPhase == AllDone {
			mu.Unlock()
			break
		}
		for _, v := range c.JobMetaHolder.MetaMap {
			if v.state == Working {
			}

			if v.state == Working && time.Since(v.StartTime) > 9*time.Second {
				switch v.JobAdr.JobType {
				case mapJob:
					c.mapJobChannel <- v.JobAdr
					v.state = Waiting
				case reduceJob:
					c.reduceJobChannel <- v.JobAdr
					v.state = Waiting

				}
			}
		}
		mu.Unlock()
	}

}

func (c *Coordinator) makemapJobs(files []string) {
	for _, v := range files {
		id := c.generateJobId()
		Job := Job{
			JobType:    mapJob,
			JobId:      id,
			ReducerNum: c.ReducerNum,
			FileSlice:  []string{v},
		}
		JobMetaInfo := JobMetaInfo{
			state:  Waiting,
			JobAdr: &Job,
		}
		c.JobMetaHolder.acceptMeta(&JobMetaInfo)
		c.mapJobChannel <- &Job
	}
}

func (c *Coordinator) makereduceJobs() {
	for i := 0; i < c.ReducerNum; i++ {
		id := c.generateJobId()
		Job := Job{
			JobId:     id,
			JobType:   reduceJob,
			FileSlice: selectReduceName(i),
		}
		JobMetaInfo := JobMetaInfo{
			state:  Waiting,
			JobAdr: &Job,
		}
		c.JobMetaHolder.acceptMeta(&JobMetaInfo)
		c.reduceJobChannel <- &Job
	}
}

func selectReduceName(reduceNum int) []string {
	var s []string
	path, _ := os.Getwd()
	files, _ := ioutil.ReadDir(path)
	for _, fi := range files {
		// 匹配对应的reduce文件
		if strings.HasPrefix(fi.Name(), "mr-tmp") && strings.HasSuffix(fi.Name(), strconv.Itoa(reduceNum)) {
			s = append(s, fi.Name())
		}
	}
	return s
}

// 将接受JobMetaInfo储存进MetaHolder里
func (t *JobMetaHolder) acceptMeta(JobInfo *JobMetaInfo) bool {
	JobId := JobInfo.JobAdr.JobId
	meta, _ := t.MetaMap[JobId]
	if meta != nil {
		//fmt.Println("meta contains Job which id = ", JobId)
		return false
	} else {
		t.MetaMap[JobId] = JobInfo
	}
	return true
}

// 分发任务
func (c *Coordinator) PollJob(args *JobArgs, reply *Job) error {
	mu.Lock()
	defer mu.Unlock()

	// 判断任务类型存任务
	switch c.DistPhase {
	case MapPhase:
		{
			if len(c.mapJobChannel) > 0 {
				*reply = *<-c.mapJobChannel
				if !c.JobMetaHolder.judgeState(reply.JobId) {
					fmt.Printf("Map-Jobid[ %d ] is running\n", reply.JobId)
				}
			} else {
				reply.JobType = waittingJob // 如果map任务被分发完了但是又没完成，此时就将任务设为Waitting
				if c.JobMetaHolder.checkJobDone() {
					c.makereduceJobs()
					c.DistPhase = ReducePhase
				}
				return nil
			}
		}

	case ReducePhase:
		{
			if len(c.reduceJobChannel) > 0 {
				*reply = *<-c.reduceJobChannel
				if !c.JobMetaHolder.judgeState(reply.JobId) {
					fmt.Printf("Reduce-Jobid[ %d ] is running\n", reply.JobId)
				}
			} else {
				reply.JobType = waittingJob // 如果map任务被分发完了但是又没完成，此时就将任务设为Waitting
				if c.JobMetaHolder.checkJobDone() {
					c.DistPhase = AllDone
				}
				return nil
			}
		}

	case AllDone:
		{
			reply.JobType = exitJob
		}
	}

	return nil
}

// 检查多少个任务做了包括（map、reduce）,
func (t *JobMetaHolder) checkJobDone() bool {

	var (
		mapDoneNum      = 0
		mapUnDoneNum    = 0
		reduceDoneNum   = 0
		reduceUnDoneNum = 0
	)
	for _, v := range t.MetaMap {
		// 首先判断任务的类型
		if v.JobAdr.JobType == mapJob {
			// 判断任务是否完成,下同
			if v.state == Done {
				mapDoneNum++
			} else {
				mapUnDoneNum++
			}
		} else if v.JobAdr.JobType == reduceJob {
			if v.state == Done {
				reduceDoneNum++
			} else {
				reduceUnDoneNum++
			}
		}

	}
	if (mapDoneNum > 0 && mapUnDoneNum == 0) && (reduceDoneNum == 0 && reduceUnDoneNum == 0) {
		return true
	} else {
		if reduceDoneNum > 0 && reduceUnDoneNum == 0 {
			return true
		}
	}

	return false

}

// 判断给定任务是否在工作，并修正其目前任务信息状态,如果任务不在工作的话返回true
func (t *JobMetaHolder) judgeState(JobId int) bool {
	JobInfo, ok := t.MetaMap[JobId]
	if !ok || JobInfo.state != Waiting {
		return false
	}
	JobInfo.state = Working
	JobInfo.StartTime = time.Now()
	return true
}

// 通过结构体的JobId自增来获取唯一的任务id
func (c *Coordinator) generateJobId() int {

	res := c.JobId
	c.JobId++
	return res
}

func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) MarkFinished(args *Job, reply *Job) error {
	mu.Lock()
	defer mu.Unlock()
	switch args.JobType {
	case mapJob:
		meta, ok := c.JobMetaHolder.MetaMap[args.JobId]

		//prevent a duplicated work which returned from another worker
		if ok && meta.state == Working {
			meta.state = Done
		}
		break
	case reduceJob:
		meta, ok := c.JobMetaHolder.MetaMap[args.JobId]

		//prevent a duplicated work which returned from another worker
		if ok && meta.state == Working {
			meta.state = Done
		}
		break

	default:
		panic("The Job type undefined ! ! !")
	}
	return nil

}

// Done 主函数mr调用，如果所有Job完成mr会通过此方法退出
func (c *Coordinator) Done() bool {
	mu.Lock()
	defer mu.Unlock()
	if c.DistPhase == AllDone {
		return true
	} else {
		return false
	}

}

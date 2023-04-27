package mr

import (
	"os"
	"strconv"
)

// Job worker向coordinator获取Job的结构体
type Job struct {
	JobType    JobType  // 任务类型
	JobId      int      // 任务id
	ReducerNum int      // reducer的数量
	FileSlice  []string // 输入文件的切片
}

// JobArgs rpc应该传入的参数，可实际上应该什么都不用传,因为只是worker获取一个任务
type JobArgs struct{}

// JobType 对于下方枚举任务的父类型
type JobType int

// Phase 对于分配任务阶段的父类型
type Phase int

// State 任务的状态的父类型
type State int

// 枚举任务的类型
const (
	mapJob JobType = iota
	reduceJob
	waittingJob // Waittingen任务代表此时为任务都分发完了，但是任务还没完成，阶段未改变
	exitJob     // exit
)

// 枚举阶段的类型
const (
	MapPhase    Phase = iota // 此阶段在分发MapJob
	ReducePhase              // 此阶段在分发ReduceJob
	AllDone                  // 此阶段已完成
)

// 任务状态类型
const (
	Working State = iota // 此阶段在工作
	Waiting              // 此阶段在等待执行
	Done                 // 此阶段已经做完
)

func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

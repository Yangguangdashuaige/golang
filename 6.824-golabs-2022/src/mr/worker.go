package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	//CallExample()
	keepFlag := true
	for keepFlag {

		Job := GetJob()
		switch Job.JobType {
		case mapJob:
			{
				DomapJob(mapf, &Job)
				Jobdone(&Job)
			}

		case waittingJob:
			{
				time.Sleep(time.Second * 5)
			}

		case reduceJob:
			{
				DoreduceJob(reducef, &Job)
				Jobdone(&Job)
			}

		case exitJob:
			{
				time.Sleep(time.Second)
				fmt.Println("All Jobs are Done ,will be exiting...")
				keepFlag = false
			}

		}
	}

	time.Sleep(time.Second)
	// uncomment to send the Example RPC to the coordinator.

}

func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	//fmt.Println(err)
	return false
}

// GetJob 获取Job
func GetJob() Job {
	args := JobArgs{}
	reply := Job{}
	call("Coordinator.PollJob", &args, &reply)
	return reply
}

func DomapJob(mapf func(string, string) []KeyValue, response *Job) {
	var intermediate []KeyValue
	FileSlice := response.FileSlice
	v := FileSlice[0]
	file, err := os.Open(v)
	if err != nil {
		log.Fatalf("cannot open %v", FileSlice)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", FileSlice)
	}
	file.Close()
	intermediate = mapf(v, string(content)) // map返回一组KV结构体数组
	rn := response.ReducerNum
	HashedKV := make([][]KeyValue, rn)
	for _, kv := range intermediate {
		HashedKV[ihash(kv.Key)%rn] = append(HashedKV[ihash(kv.Key)%rn], kv)
	}
	for i := 0; i < rn; i++ {
		oname := "mr-tmp-" + strconv.Itoa(response.JobId) + "-" + strconv.Itoa(i)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range HashedKV[i] {
			enc.Encode(kv)
		}
		ofile.Close()
	}
}

func DoreduceJob(reducef func(string, []string) string, response *Job) {
	reduceFileNum := response.JobId
	files := response.FileSlice
	var intermediate []KeyValue
	for _, filepath := range files {
		file, _ := os.Open(filepath)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(intermediate))
	dir, _ := os.Getwd()
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create temp file", err)
	}
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	tempFile.Close()
	fn := fmt.Sprintf("mr-out-%d", reduceFileNum)
	os.Rename(tempFile.Name(), fn)
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func Jobdone(f *Job) Job {
	args := f
	reply := Job{}
	call("Coordinator.MarkFinished", &args, &reply)
	return reply
}

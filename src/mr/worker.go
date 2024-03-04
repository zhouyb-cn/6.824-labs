package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
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

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()

	GetTaskFromCor(mapf, reducef)

}

func GetTaskFromCor(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// 先执行map任务
	for {
		args := ExampleArgs{}
		reply := DistributeTaskReply{}
		// send the RPC request, wait for the reply.
		// the "Coordinator.Example" tells the
		// receiving server that we'd like to call
		// the Example() method of struct Coordinator.
		ok := call("Coordinator.DistributeTask", &args, &reply)
		if !ok {
			// TODO 处理远程调用失败
			fmt.Printf("call DistributeTask failed!\n")
			return
		}
		switch reply.TaskType {
		case MapTask:
			Print("------> map task", reply.Index)
			doMapTask(reply.Index, reply.Task, mapf, reply.ReduceNum)
		case ReduceTask:
			Print("------> reduce task", reply.Index)
			doReduceTask(reply.Index, reply.Task, reducef)
		case WaitingTask:
			// 任务还没全部完成，需要等待
			Print("------> waiting task")
			time.Sleep(1 * time.Second)
		case DoneTask:
			Print("all task is done")
			return
		default:
			Print("------> task type err")
			time.Sleep(1 * time.Second)
		}

		//
		// a big difference from real MapReduce is that all the
		// intermediate data is in one place, intermediate[],
		// rather than being partitioned into NxM buckets.
		//
	}
	return
}

func doMapTask(index int, task *Task, mapf func(string, string) []KeyValue, nReduce int) {
	// 分片存储
	Print("do map task")
	intermediates := make([][]KeyValue, nReduce)
	for _, filename := range task.Files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()

		kva := mapf(filename, string(content))
		for _, v := range kva {
			idx := ihash(v.Key) % nReduce
			intermediates[idx] = append(intermediates[idx], v)
		}
	}
	reduceFiles := make(map[int]string)
	for y, intermediate := range intermediates {
		if len(intermediate) == 0 {
			continue
		}
		sort.Sort(ByKey(intermediate))

		oname := fmt.Sprintf("mr-%d-%d", index, y)
		ofile, _ := os.Create(oname)

		enc := json.NewEncoder(ofile)
		for _, kv := range intermediate {
			err := enc.Encode(&kv)
			if err != nil {
				Print("enc encode err, ", err.Error())
			}
		}
		reduceFiles[y] = oname
	}
	// call rpc task is done
	args := TaskDoneArgs{
		TaskType:    MapTask,
		Index:       index,
		ReduceFiles: reduceFiles,
	}
	reply := TaskDoneReply{}
	ok := call("Coordinator.TaskDone", &args, &reply)
	if !ok {
		// TODO 处理远程调用失败
		fmt.Printf("call TaskDone failed!\n")
		return
	}
}

func doReduceTask(index int, task *Task, reducef func(string, []string) string) {
	Print("do reduce task")
	intermediate := make([]KeyValue, 0)
	fileMap := make(map[string]struct{})
	for _, filename := range task.Files {
		if _, ok := fileMap[filename]; ok {
			continue
		}
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
		fileMap[filename] = struct{}{}
	}
	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%d", index)
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()

	// call rpc
	args := TaskDoneArgs{
		TaskType: ReduceTask,
		Index:    index,
	}
	reply := TaskDoneReply{}
	ok := call("Coordinator.TaskDone", &args, &reply)
	if !ok {
		// TODO 处理远程调用失败
		fmt.Printf("call TaskDone failed!\n")
		return
	}

	return
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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

	Print(err)
	return false
}

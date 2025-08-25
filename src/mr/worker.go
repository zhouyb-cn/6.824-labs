package mr

import (
	"encoding/json"
	"fmt"
	"io"
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

// for sorting by key.
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

	for {
		args := TaskArgs{}
		reply := TaskReply{}
		ok := call("Coordinator.DistributeTask", &args, &reply)
		if ok {
			if reply.Action == Quit {
				break
			} else if reply.Action == Wait {
				time.Sleep(1 * time.Second)
				continue
			}
			switch reply.Task.TaskType {
			case TaskMap:
				doMapTask(reply.Task, mapf)
			case TaskReduce:
				doReduceTask(reply.Task, reducef)
			}
		}
		time.Sleep(1 * time.Second)
	}

}

func doMapTask(task Task, mapf func(string, string) []KeyValue) {
	file, err := os.Open(task.File)
	if err != nil {
		log.Fatalf("cannot open %v", task.File)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.File)
	}
	kvs := mapf(task.File, string(content))
	results := make([][]KeyValue, task.NReduce)
	for _, kv := range kvs {
		h := ihash(kv.Key) % task.NReduce
		results[h] = append(results[h], kv)
	}
	for i, result := range results {
		tmpFile, err := os.CreateTemp(".", fmt.Sprintf("mr-tmp-%d-%d", task.Index, i))
		if err != nil {
			log.Fatalf("CreateTemp err %v", err)
		}
		enc := json.NewEncoder(tmpFile)
		for _, kv := range result {
			if err := enc.Encode(&kv); err != nil {
				log.Fatalf("写入失败: %v", err)
			}
		}
		tmpFile.Close()
		if err := os.Rename(tmpFile.Name(), fmt.Sprintf("mr-%d-%d", task.Index, i)); err != nil {
			log.Fatalf("重命名失败: %v", err)
		}
	}
	args := TaskResultArgs{TaskType: TaskMap, Index: task.Index}
	reply := TaskResultReply{}
	ok := call("Coordinator.UpdateTaskResult", &args, &reply)
	if !ok {
		log.Fatal("rpc error1")
	}
}

func doReduceTask(task Task, reducef func(string, []string) string) {
	intermediate := []KeyValue{}
	for i := range task.NMap {
		file, _ := os.Open(fmt.Sprintf("mr-%d-%d", i, task.Index))
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
	tmpFile, _ := os.CreateTemp(".", fmt.Sprintf("tmp_mr_out_%d", task.Index))
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
		fmt.Fprintf(tmpFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	tmpFile.Close()
	os.Rename(tmpFile.Name(), fmt.Sprintf("mr-out-%d", task.Index))
	args := TaskResultArgs{TaskType: TaskReduce, Index: task.Index}
	reply := TaskResultReply{}
	ok := call("Coordinator.UpdateTaskResult", &args, &reply)
	if !ok {
		log.Fatal("rpc error2")
	}
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

	fmt.Println(err)
	return false
}

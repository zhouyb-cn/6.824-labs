package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type DistributeTaskArgs struct {
}

type DistributeTaskReply struct {
	TaskType  string `json:"task_type"`
	Index     int    `json:"index"`
	Task      *Task  `json:"task"`
	ReduceNum int    `json:"reduce_num"`
}

type TaskDoneArgs struct {
	TaskType    string
	Index       int
	ReduceFiles map[int]string
}

type TaskDoneReply struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

// 任务状态
const (
	Init       = "init"
	Processing = "processing"
	Done       = "done"
)

const MaxWaitTime = 10

// 任务类型
const (
	MapTask     = "map_task"
	ReduceTask  = "reduce_task"
	WaitingTask = "waiting_task"
	DoneTask    = "done_task"
)

type Coordinator struct {
	// Your definitions here.
	MapTasks    []*Task
	ReduceTasks []*Task
	ReduceNum   int
	sync.Mutex
	MapTaskDone    bool // 全部map任务完成
	ReduceTaskDone bool // 全部reduce任务完成
}

type Task struct {
	// 任务状态
	Status string
	// 要处理的文件
	Files []string
	// 任务分配时间
	StartTime time.Time
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) DistributeTask(args *ExampleArgs, reply *DistributeTaskReply) error {
	defer c.Unlock()
	c.Lock()

	if !c.MapTaskDone {
		for index, task := range c.MapTasks {
			// 有待分发任务 直接返回任务给worker
			if c.MapTasks[index].Status == Init ||
				(task.Status == Processing && time.Now().Sub(task.StartTime).Seconds() > MaxWaitTime) {
				reply.TaskType = MapTask
				c.MapTasks[index].StartTime = time.Now()
				reply.Task = task
				reply.Index = index
				reply.ReduceNum = c.ReduceNum
				c.MapTasks[index].Status = Processing
				Print("distribute map task, ", index)
				return nil
			} else {
				continue
			}
		}
		// 没有待分配的任务 但是全部map任务还没有完成时
		reply.TaskType = WaitingTask
		return nil
	} else if !c.ReduceTaskDone {
		for index, task := range c.ReduceTasks {
			if task.Status == Init ||
				(task.Status == Processing && time.Now().Sub(task.StartTime).Seconds() > MaxWaitTime) {
				c.ReduceTasks[index].StartTime = time.Now()
				reply.TaskType = ReduceTask
				reply.Task = task
				reply.Index = index
				c.ReduceTasks[index].Status = Processing
				Print("distribute reduce task, ", index)
				return nil
			} else {
				continue
			}
		}
		reply.TaskType = WaitingTask
		return nil
	}

	reply.TaskType = DoneTask

	return nil
}

func (c *Coordinator) TaskDone(args TaskDoneArgs, reply *TaskDoneReply) error {
	if args.TaskType == MapTask {
		Print("map task done,", args.Index)
		c.MapTasks[args.Index].Status = Done
		reduceFiles := args.ReduceFiles
		for k, v := range reduceFiles {
			c.ReduceTasks[k].Files = append(c.ReduceTasks[k].Files, v)
		}
	} else if args.TaskType == ReduceTask {
		Print("reduce task done,", args.Index)
		for _, filename := range c.ReduceTasks[args.Index].Files {
			// 删除中间文件
			os.Remove(filename)
		}
		c.ReduceTasks[args.Index].Status = Done
	}

	var allMapTasksIsDone, allReduceTasksIsDone = true, true
	for k, _ := range c.MapTasks {
		if c.MapTasks[k].Status != Done {
			allMapTasksIsDone = false
			break
		}
	}
	for k, _ := range c.ReduceTasks {
		if c.ReduceTasks[k].Status != Done {
			allReduceTasksIsDone = false
			break
		}
	}
	c.MapTaskDone = allMapTasksIsDone
	c.ReduceTaskDone = allReduceTasksIsDone
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	if c.MapTaskDone && c.ReduceTaskDone {
		ret = true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.ReduceNum = nReduce

	// Your code here.
	c.MapTasks = make([]*Task, len(files))
	for index, file := range files {
		task := Task{
			Status: Init,
			Files:  []string{file},
		}
		c.MapTasks[index] = &task
	}

	c.ReduceTasks = make([]*Task, nReduce)
	for i := 0; i < nReduce; i++ {
		task := Task{
			Status: Init,
		}
		c.ReduceTasks[i] = &task
	}

	c.server()
	return &c
}

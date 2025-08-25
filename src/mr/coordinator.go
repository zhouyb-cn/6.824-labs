package mr

import (
	"fmt"
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

// 动作
const (
	Quit = "quit"
	Wait = "wait"
)

// 任务类型
const (
	TaskMap    = "map_task"
	TaskReduce = "reduce_task"
)

type Coordinator struct {
	// Your definitions here.
	Files       []string `json:"files"`
	MapTasks    []Task   `json:"map_tasks"`
	ReduceTasks []Task   `json:"reduce_tasks"`
	// reduce numbers
	NReduce        int  `json:"n_reduce"`
	AllMapTaskDone bool `json:"all_map_task_done"`
	AllDone        bool `json:"all_done"`

	// lock
	Mutex sync.Mutex
}

type Task struct {
	Index     int       `json:"index"`
	File      string    `json:"file"`
	Status    string    `json:"status"`
	StartTime time.Time `json:"start_time"`
	NReduce   int       `json:"n_reduce"`
	NMap      int       `json:"n_map"`
	TaskType  string    `json:"task_type"`
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) DistributeTask(args *TaskArgs, reply *TaskReply) error {
	// need lock first
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	nt := time.Now()
	// first distribute map task
	if !c.AllMapTaskDone {
		for index, task := range c.MapTasks {
			if task.Status == Init || (task.Status == Processing && nt.Sub(task.StartTime).Seconds() > 10) {
				c.MapTasks[index].Status = Processing
				c.MapTasks[index].StartTime = nt
				reply.Task = task
				return nil
			}
		}
	} else {
		for index, task := range c.ReduceTasks {
			if task.Status == Init || (task.Status == Processing && nt.Sub(task.StartTime).Seconds() > 10) {
				c.ReduceTasks[index].Status = Processing
				c.ReduceTasks[index].StartTime = nt
				reply.Task = task
				return nil
			}
		}
	}
	if c.AllDone {
		reply.Action = Quit
		return nil
	}
	reply.Action = Wait
	return nil
}

func (c *Coordinator) UpdateTaskResult(args *TaskResultArgs, reply *TaskResultReply) error {
	// need lock first
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	if args.TaskType == TaskMap {
		c.MapTasks[args.Index].Status = Done
		c.AllMapTaskDone = true
		for _, task := range c.MapTasks {
			if task.Status == Init || task.Status == Processing {
				c.AllMapTaskDone = false
				break
			}
		}
	} else {
		c.ReduceTasks[args.Index].Status = Done
		c.AllDone = true
		for _, task := range c.ReduceTasks {
			if task.Status == Init || task.Status == Processing {
				c.AllDone = false
				break
			}
		}
		// 所有reduce任务完成后，删除中间文件
		if c.AllDone {
			for i := 0; i < len(c.MapTasks); i++ {
				for j := 0; j < c.NReduce; j++ {
					os.Remove(fmt.Sprintf("mr-%d-%d", i, j))
				}
			}
		}
	}
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
	if c.AllDone {
		ret = true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.Files = files
	c.NReduce = nReduce

	c.MapTasks = make([]Task, 0, len(files))
	for index, file := range files {
		c.MapTasks = append(c.MapTasks, Task{
			Index:    index,
			File:     file,
			Status:   Init,
			NReduce:  nReduce,
			TaskType: TaskMap,
		})
	}

	c.ReduceTasks = make([]Task, 0, nReduce)
	for i := range nReduce {
		c.ReduceTasks = append(c.ReduceTasks, Task{
			Index:    i,
			Status:   Init,
			TaskType: TaskReduce,
			NMap:     len(files),
		})
	}

	c.server()
	return &c
}

package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type taskState int

const (
	assignable taskState = iota
	processing
	finished
)

type mapTask struct {
	taskState
	filename string
	nBucket  int
}

type reduceTask struct {
	taskState
}

type Coordinator struct {
	// Your definitions here.
	files   []string
	timeout time.Duration
	mtx     sync.Mutex

	/* use for Map tasks */
	nMap         int
	nMapFinished int
	mapTasks     []*mapTask
	curMapIdx    int

	/* use for Reduce tasks */
	nReduce         int
	nReduceFinished int
	reduceTasks     []*reduceTask
	curReduceIdx    int
}

func (c *Coordinator) assginMapTask(idx int, args *TaskArgs, reply *TaskReply) error {
	log.Println("[assgin Map]", idx)

	curTask := c.mapTasks[idx]
	curTask.taskState = processing
	*reply = TaskReply{
		TaskType: MapType,
		TaskID:   idx,
		Filename: curTask.filename,
		Nreduce:  curTask.nBucket,
	}
	return nil
}

func (c *Coordinator) assginReduceTask(idx int, args *TaskArgs, reply *TaskReply) error {
	log.Println("[assign Reduce]", idx)

	curTask := c.reduceTasks[idx]
	curTask.taskState = processing
	*reply = TaskReply{
		TaskType: ReduceType,
		TaskID:   idx,
		Nmap:     len(c.files),
	}
	return nil
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AssignTask(args *TaskArgs, reply *TaskReply) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if c.nMapFinished < c.nMap {
		i := c.curMapIdx
		for j := 0; j < c.nMap; j++ {
			if c.mapTasks[i].taskState == assignable {
				break
			}
			i = (i + 1) % c.nMap
		}
		if c.mapTasks[i].taskState == assignable {
			c.assginMapTask(i, args, reply)
			c.curMapIdx = (i + 1) % c.nMap
			go func() {
				t := time.NewTimer(c.timeout)
				// log.Println("timer begin:", i)
				defer t.Stop()
				<-t.C
				c.mtx.Lock()
				if c.mapTasks[i].taskState != finished {
					c.mapTasks[i].taskState = assignable
					log.Println("[timeout Map]", i)
				}
				c.mtx.Unlock()
			}()
		} else { // wait for processing task
			*reply = TaskReply{
				TaskType: TaskPending,
			}
		}
	} else if c.nReduceFinished < c.nReduce {
		i := c.curReduceIdx
		for j := 0; j < c.nReduce; j++ {
			if c.reduceTasks[i].taskState == assignable {
				break
			}
			i = (i + 1) % c.nReduce
		}
		if c.reduceTasks[i].taskState == assignable {
			c.assginReduceTask(i, args, reply)
			c.curReduceIdx = (i + 1) % c.nReduce
			go func() {
				t := time.NewTimer(c.timeout)
				defer t.Stop()
				<-t.C
				c.mtx.Lock()
				if c.reduceTasks[i].taskState != finished {
					c.reduceTasks[i].taskState = assignable
					log.Println("[timeout Reduce]", i)
				}
				c.mtx.Unlock()
			}()
		} else { // wait for processing task
			*reply = TaskReply{
				TaskType: TaskPending,
			}
		}
	} else {
		*reply = TaskReply{
			TaskType: TaskDone,
		}
	}
	return nil
}

func (c *Coordinator) NotifyTask(args *NotifyArgs, reply *NotifyReply) error {
	if args.NotifyErr != "" {
		log.Println("notify task error")
		return nil
	}
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if args.NotifyType == MapType {
		idx := args.NotifyID
		if c.mapTasks[idx].taskState == finished {
			reply.Err = errors.New("repeat notify finished map task").Error()
		} else if c.mapTasks[idx].taskState == assignable {
			reply.Err = errors.New("notify assignable map task").Error()
		} else {
			c.mapTasks[idx].taskState = finished
			c.nMapFinished++
			log.Println("[finished Map]", idx)
		}
	} else if args.NotifyType == ReduceType {
		idx := args.NotifyID
		if c.reduceTasks[idx].taskState == finished {
			reply.Err = errors.New("repeat notify finished reduce task").Error()
		} else if c.reduceTasks[idx].taskState == assignable {
			reply.Err = errors.New("notify assignable reduce task").Error()
		} else {
			c.reduceTasks[idx].taskState = finished
			c.nReduceFinished++
			log.Println("[finished Reduce]", idx)
		}
	} else {
		reply.Err = errors.New("notify bad task type").Error()
	}
	if reply.Err != "" {
		fmt.Println("notify reply.err", reply.Err)
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.mtx.Lock()
	ret = c.nReduceFinished == c.nReduce
	c.mtx.Unlock()

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:   files,
		timeout: time.Second * 10,
		nMap:    len(files),
		nReduce: nReduce,
	}
	c.mapTasks = make([]*mapTask, c.nMap)
	for i := 0; i < c.nMap; i++ {
		c.mapTasks[i] = &mapTask{
			taskState: assignable,
			filename:  files[i],
			nBucket:   nReduce,
		}
	}
	c.reduceTasks = make([]*reduceTask, c.nReduce)
	for i := 0; i < c.nReduce; i++ {
		c.reduceTasks[i] = &reduceTask{
			taskState: assignable,
		}
	}

	c.server()
	return &c
}

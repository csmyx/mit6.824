package mr

import (
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	/* use for Map stack */
	files         []string
	unMapIdx      int
	startedMapSet sync.Map
	failedMapSet  map[int]struct{}
	mapChs        []chan struct{}
	mapTimeout    time.Duration

	/* use for Reduce stack */
	nReduce          int
	unReduceIdx      int
	startedReduceSet map[int]struct{}
	reduceChs        []chan struct{}
	failedReduceSet  map[int]struct{}
	reduceTimeout    time.Duration

	mtx sync.Mutex
}

func readFile(filename string) ([]byte, error) {
	file, err := os.Open(filename)
	defer func() {
		if err != nil {
			file.Close()
		}
	}()
	if err != nil {
		log.Printf("cannot open %v", filename)
		return nil, err
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Printf("cannot read %v", filename)
		return nil, err
	}
	return content, nil
}

func (c *Coordinator) assginMapWork(idx int, args *RPCArgs, reply *RPCReply) error {
	c.startedMapSet[idx] = struct{}{} // 标记当前任务开始
	filename := c.files[idx]
	content, err := readFile(filename)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			delete(c.startedMapSet, idx) // 标记当前任务结束
			return
		}
		go func() {
			select {
			case <-time.After(c.mapTimeout):
				c.mtx.Lock()
				c.failedMapSet[idx] = struct{}{}

				c.mtx.Unlock()
			case <-c.mapChs[idx]:
				// do nothing
			}
			c.mtx.Lock()
			delete(c.startedMapSet, idx) // 标记当前任务结束
			c.mtx.Unlock()
		}()
	}()
	reply.TaskType = MapType
	reply.TaskID = idx
	reply.Filename = filename
	reply.Content = content
	reply.Nreduce = c.nReduce
	return nil
}

func (c *Coordinator) assginReduceWork(idx int, args *RPCArgs, reply *RPCReply) error {
	c.startedReduceSet[idx] = struct{}{} // 标记当前任务开始
	defer func() {
		go func() {
			select {
			case <-time.After(c.reduceTimeout):
				c.mtx.Lock()
				c.failedReduceSet[idx] = struct{}{}
				c.mtx.Unlock()
			case <-c.reduceChs[idx]:
				// do nothing
			}
			c.mtx.Lock()
			delete(c.startedReduceSet, idx) // 标记当前任务结束
			c.mtx.Unlock()
		}()
	}()
	reply.TaskType = ReduceType
	reply.TaskID = idx
	reply.Nmap = len(c.files)
	return nil
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) UnstartedTask(args *RPCArgs, reply *RPCReply) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if c.unMapIdx < len(c.files) { // Map task
		err := c.assginMapWork(c.unMapIdx, args, reply)
		c.unMapIdx++
		return err
	} else if len(c.failedMapSet) != 0 {
		var idx int
		for k := range c.failedMapSet {
			idx = k
			break
		}
		delete(c.failedMapSet, idx)
		return c.assginMapWork(idx, args, reply)
	} else if c.unReduceIdx != c.nReduce { // Reduce task
		err := c.assginReduceWork(c.unReduceIdx, args, reply)
		c.unReduceIdx++
		return err
	} else if len(c.failedReduceSet) != 0 {
		var idx int
		for k := range c.failedReduceSet {
			idx = k
			break
		}
		delete(c.failedReduceSet, idx)
		return c.assginReduceWork(idx, args, reply)
	} else {
		reply.TaskType = NoType
		return nil
	}
}

func (c *Coordinator) NotifyFinish(args *NotifyArgs, reply *NotifyReply) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if args.NotifyType == MapType {
		idx := args.NotifyID
		if _, ok := c.startedMapSet[idx]; !ok {
			return BadMapNotify
		}
		c.mapChs[idx] <- struct{}{} // 标记任务已完成
		return nil
	} else if args.NotifyType == ReduceType {
		idx := args.NotifyID
		if _, ok := c.startedReduceSet[idx]; !ok {
			return BadReduceNotify
		}
		c.reduceChs[idx] <- struct{}{} // 标记任务已完成
		return nil
	}
	return BadNotify
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

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.files = make([]string, len(files))
	copy(c.files, files)
	c.startedMapSet = make(map[int]struct{})
	c.failedMapSet = make(map[int]struct{})
	c.mapChs = make([]chan struct{}, len(files))
	c.mapTimeout = time.Second * 10

	c.nReduce = nReduce
	c.startedReduceSet = make(map[int]struct{})
	c.failedReduceSet = make(map[int]struct{})
	c.reduceChs = make([]chan struct{}, c.nReduce)
	c.reduceTimeout = time.Second * 10

	c.server()
	return &c
}

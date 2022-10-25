package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type mapTaskT struct {
	id       int
	filename string
}

type reduceTaskT struct {
	id int
}

type Coordinator struct {
	// Your definitions here.
	files   []string
	timeout time.Duration

	/* use for Map tasks */
	nMap         int
	mapTaskQ     chan *mapTaskT
	mapNotifyChs []chan struct{}
	mapDoneChs   []chan struct{}

	/* use for Reduce tasks */
	nReduce         int
	reduceTaskQ     chan *reduceTaskT
	reduceNotifyChs []chan struct{}
	reduceDoneChs   []chan struct{}

	done chan struct{}
}

func (c *Coordinator) assginMapTask(task *mapTaskT, args *TaskArgs, reply *TaskReply) error {
	log.Println("--- [assgin Map]", task.id)

	*reply = TaskReply{
		TaskType: MapType,
		TaskID:   task.id,
		Filename: task.filename,
		Nreduce:  c.nReduce,
	}
	return nil
}

func (c *Coordinator) assginReduceTask(task *reduceTaskT, args *TaskArgs, reply *TaskReply) error {
	log.Println("--- [assgin Reduce]", task.id)

	*reply = TaskReply{
		TaskType: ReduceType,
		TaskID:   task.id,
		Nmap:     c.nMap,
	}

	return nil
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AssignTask(args *TaskArgs, reply *TaskReply) error {
	if task, ok := <-c.mapTaskQ; ok { // process map task
		if err := c.assginMapTask(task, args, reply); err != nil {
			return err
		}
		task := task
		go func() {
			t := time.NewTimer(c.timeout)
			defer t.Stop()
			select {
			case <-c.mapNotifyChs[task.id]:
				close(c.mapDoneChs[task.id])
				log.Println("*** [notified Map]", task.id)
			case <-t.C:
				c.mapTaskQ <- task // put back to task-queue
				log.Println("^^^^^ [timeout Map]", task.id)
			}
		}()
		return nil
	}
	if task, ok := <-c.reduceTaskQ; ok { // process reduce task
		if err := c.assginReduceTask(task, args, reply); err != nil {
			return err
		}
		task := task
		go func() {
			t := time.NewTimer(c.timeout)
			defer t.Stop()
			select {
			case <-c.reduceNotifyChs[task.id]:
				close(c.reduceDoneChs[task.id])
				log.Println("*** [notified reduce]", task.id)
			case <-t.C:
				c.reduceTaskQ <- task // put back to task-queue
				log.Println("^^^^^ [timeout reduce]", task.id)
			}
		}()
		return nil
	}

	reply.TaskType = TaskDone
	return nil
}

func (c *Coordinator) NotifyTask(args *NotifyArgs, reply *NotifyReply) error {
	if args.NotifyErr != "" {
		return nil
	}
	id := args.TaskID
	switch args.TaskType {
	case MapType:
		select {
		case c.mapNotifyChs[id] <- struct{}{}:
		case <-c.mapDoneChs[id]:
			reply.Err = "repeat notify map"
			log.Println("^^^^^ [repeat notify Map]", id)
		}
	case ReduceType:
		select {
		case c.reduceNotifyChs[id] <- struct{}{}:
		case <-c.reduceDoneChs[id]:
			reply.Err = "repeat notify reduece"
			log.Println("^^^^^ [repeat notify Reduce]", id)
		}
	default:
		reply.Err = "bad notify type"
		// log.Println("bad notify type")
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
	if _, ok := <-c.done; !ok {
		ret = true
	} else {
		c.done <- struct{}{}
	}

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
	c.mapTaskQ = make(chan *mapTaskT, c.nMap)
	c.mapNotifyChs = make([]chan struct{}, c.nMap)
	c.mapDoneChs = make([]chan struct{}, c.nMap)
	for i := 0; i < c.nMap; i++ {
		c.mapTaskQ <- &mapTaskT{
			id:       i,
			filename: files[i],
		}
		c.mapNotifyChs[i] = make(chan struct{})
		c.mapDoneChs[i] = make(chan struct{})
	}
	c.reduceTaskQ = make(chan *reduceTaskT, c.nReduce)
	c.reduceNotifyChs = make([]chan struct{}, c.nReduce)
	c.reduceDoneChs = make([]chan struct{}, c.nReduce)
	for i := 0; i < c.nReduce; i++ {
		c.reduceTaskQ <- &reduceTaskT{
			id: i,
		}
		c.reduceNotifyChs[i] = make(chan struct{})
		c.reduceDoneChs[i] = make(chan struct{})
	}

	c.done = make(chan struct{}, 1)
	c.done <- struct{}{}
	c.server()

	go func() {
		for i := 0; i < c.nMap; i++ {
			<-c.mapDoneChs[i]
		}
		close(c.mapTaskQ)
		for i := 0; i < c.nReduce; i++ {
			<-c.reduceDoneChs[i]
		}
		close(c.reduceTaskQ)
		close(c.done) // indicate all tasks has been done
	}()
	return &c
}

package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"errors"
	"os"
	"strconv"
)

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

// Add your RPC definitions here.
const (
	NoType int = iota
	MapType
	ReduceType
)

type RPCArgs struct {
	X int
}

type RPCReply struct {
	TaskType int
	TaskID   int

	/* use for Map task */
	Filename string
	Content  []byte
	Nreduce  int

	/* use for Reduce task */
	Nmap int
}

var (
	BadNotify       error = errors.New("Bad Notify")
	BadMapNotify    error = errors.New("Bad Map IDX")
	BadReduceNotify error = errors.New("Bad Reduce IDX")
)

type NotifyArgs struct {
	NotifyType int
	NotifyID   int
}

type NotifyReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

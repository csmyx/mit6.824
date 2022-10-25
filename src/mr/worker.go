package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string `json:"k"`
	Value string `json:"v"`
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func creatInterFile(mapIdx, reduceIdx int, bucket []KeyValue) error {
	filename := fmt.Sprintf("mr-%d-%d", mapIdx, reduceIdx)
	tmpFile, err := ioutil.TempFile("", filename+"-tmp-*")
	if err != nil {
		return err
	}
	for _, kv := range bucket {
		enc := json.NewEncoder(tmpFile)
		if err := enc.Encode(&kv); err != nil {
			log.Fatalf("encode err: %v\n", err)
		}
	}
	os.Rename(tmpFile.Name(), filename)
	tmpFile.Close()
	// log.Printf("Filename: %s\n", filename)
	return nil
}

func deleteInterFile(mapIdx, reduceIdx int) error {
	filename := fmt.Sprintf("mr-%d-%d", mapIdx, reduceIdx)
	if err := os.Remove(filename); err != nil {
		return err
	}
	return nil
}

func MapWorker(mapf func(string, string) []KeyValue, reply *TaskReply) error {
	log.Println("[begin Map]", reply.TaskID)
	mapID := reply.TaskID
	nReduce := reply.Nreduce
	content, err := os.ReadFile(reply.Filename)
	if err != nil {
		return err
	}
	kvs := mapf(reply.Filename, string(content))
	buckets := make([][]KeyValue, nReduce)
	for _, kv := range kvs {
		// log.Println(kv.Key, kv.Value)
		idx := ihash(kv.Key) % nReduce
		buckets[idx] = append(buckets[idx], kv)
	}
	for idx, bucket := range buckets {
		if err := creatInterFile(mapID, idx, bucket); err != nil {
			return err
			// log.Fatalf("err: %v, mapIdx: %d, reduceIdx: %d\n", err, reply.TaskID, idx)
		}
	}
	return nil
}

func ReduceWorker(reducef func(string, []string) string, reply *TaskReply) error {
	log.Println("[begin Reduce]", reply.TaskID)
	reduceID := reply.TaskID
	nMap := reply.Nmap

	kvs := []KeyValue{}
	for idx := 0; idx < nMap; idx++ {
		filename := fmt.Sprintf("mr-%d-%d", idx, reduceID)
		file, err := os.Open(filename)
		if err != nil {
			return err
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kvs = append(kvs, kv)
		}
	}
	ofilename := fmt.Sprintf("mr-out-%d", reduceID)
	// log.Printf("Output Filename: %s\n", ofilename)
	ofile, err := ioutil.TempFile("", ofilename+"-tmp-*")
	if err != nil {
		return err
	}
	sort.Sort(ByKey(kvs))
	i := 0
	for i < len(kvs) {
		values := []string{}
		j := i + 1
		for j < len(kvs) && kvs[j].Key == kvs[i].Key {
			j++
		}
		for k := i; k < j; k++ {
			values = append(values, kvs[k].Value)
		}
		output := reducef(kvs[i].Key, values)
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kvs[i].Key, output)

		i = j
	}
	os.Rename(ofile.Name(), ofilename)
	ofile.Close()
	for i := 0; i < nMap; i++ {
		if err := deleteInterFile(i, reduceID); err != nil {
			return err
		}
	}
	return nil
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		taskReply, err := getTask()
		// log.Println("process Type:", taskReply.TaskType)
		if err != nil {
			log.Fatalf("err: %v\n", err)
		}

		args := NotifyArgs{
			NotifyType: taskReply.TaskType,
			NotifyID:   taskReply.TaskID,
		}

		switch taskReply.TaskType {
		case MapType:
			if err := MapWorker(mapf, taskReply); err != nil {
				args.NotifyErr = err.Error()
			}
			notifyReply, err := notify(&args)
			if err != nil {
				log.Printf("notify err: %v\n", err)
			}
			if notifyReply.Err != "" {
				log.Println(notifyReply.Err)
			}
			log.Println("[notified Map]", taskReply.TaskID)
		case ReduceType:
			if err := ReduceWorker(reducef, taskReply); err != nil {
				args.NotifyErr = err.Error()
			}
			args := NotifyArgs{
				NotifyType: ReduceType,
				NotifyID:   taskReply.TaskID,
			}
			notifyReply, err := notify(&args)
			if err != nil {
				log.Printf("notify err: %v\n", err)
			}
			if notifyReply.Err != "" {
				log.Println(notifyReply.Err)
			}
			log.Println("[notified Reduce]", taskReply.TaskID)
		case TaskPending:
			time.Sleep(3 * time.Second)
		case TaskDone:
			log.Println("Client: All Task finished, Exit")
			goto End
		}
	}
End:
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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
	if ok == nil {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) error {
	// log.Println("call rpc:", rpcname)
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	return err
}

func getTask() (*TaskReply, error) {
	args := TaskArgs{}
	reply := TaskReply{}
	err := call("Coordinator.AssignTask", &args, &reply)
	if err != nil {
		log.Printf("call failed!\n")
		return nil, err
	}
	// log.Printf("reply.Content: %v\n", string(reply.Content))
	return &reply, nil
}

func notify(args *NotifyArgs) (*NotifyReply, error) {
	reply := NotifyReply{}
	err := call("Coordinator.NotifyTask", &args, &reply)
	if err != nil {
		log.Printf("call failed!\n")
		return nil, err
	}
	// log.Printf("reply.Content: %v\n", string(reply.Content))
	return &reply, nil
}

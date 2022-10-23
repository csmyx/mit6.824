package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
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

func genIntermFile(mapIdx, reduceIdx int, bucket []KeyValue) error {
	filename := fmt.Sprintf("mr-%d-%d", mapIdx, reduceIdx)
	log.Printf("Filename: %s\n", filename)
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			file.Close()
		}
	}()
	for _, kv := range bucket {
		enc := json.NewEncoder(file)
		if err := enc.Encode(&kv); err != nil {
			log.Fatalf("encode err: %v\n", err)
		}
	}
	return nil
}

func MapWorker(mapf func(string, string) []KeyValue, reply *RPCReply) {
	if reply.TaskType != MapType {
		log.Fatalf("Task Type error.\n")
	}
	log.Println("Task Type: MapTask.")
	mapID := reply.TaskID
	nReduce := reply.Nreduce
	kvs := mapf(reply.Filename, string(reply.Content))
	buckets := make([][]KeyValue, nReduce)
	for _, kv := range kvs {
		// log.Println(kv.Key, kv.Value)
		idx := ihash(kv.Key) % nReduce
		buckets[idx] = append(buckets[idx], kv)
	}
	for idx, bucket := range buckets {
		if err := genIntermFile(mapID, idx, bucket); err != nil {
			log.Fatalf("err: %v, mapIdx: %d, reduceIdx: %d\n", err, reply.TaskID, idx)
		}
	}
}

func ReduceWorker(reducef func(string, []string) string, reply *RPCReply) {
	if reply.TaskType != ReduceType {
		log.Fatalf("Task Type error.\n")
	}
	log.Println("Task Type: ReduceTask.")
	reduceID := reply.TaskID
	nMap := reply.Nmap

	kvs := []KeyValue{}
	for idx := 0; idx < nMap; idx++ {
		filename := fmt.Sprintf("mr-%d-%d", idx, reduceID)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("file Open err: %v", err)
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
	log.Printf("Output Filename: %s\n", ofilename)
	ofile, err := os.Create(ofilename)
	if err != nil {
		log.Fatalf("Creat %s fail, err:%v\n", ofilename, err)
	}
	defer func() {
		if err != nil {
			ofile.Close()
		}
	}()
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
		reply, err := getTask()
		if err != nil {
			log.Fatalf("err: %v\n", err)
		}
		if reply.TaskType == MapType {
			MapWorker(mapf, reply)
		} else if reply.TaskType == ReduceType {
			// id := reply.ReduceID
			ReduceWorker(reducef, reply)
		} else { // NoType
			time.Sleep(3 * time.Second)
		}
	}
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

func getTask() (*RPCReply, error) {
	args := RPCArgs{}
	reply := RPCReply{}
	err := call("Coordinator.UnstartedTask", &args, &reply)
	if err != nil {
		log.Printf("call failed!\n")
		return nil, err
	}
	// log.Printf("reply.Content: %v\n", string(reply.Content))
	return &reply, nil
}

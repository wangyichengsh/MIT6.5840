package mr

import "os"
import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "time"
import "io/ioutil"
import "encoding/json"
import "sort"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

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


//
// main/mrworker.go calls this function.
//i

func requestTask() (RequestTaskReply, bool) {
	args := RequestTaskArgs{}
	reply := RequestTaskReply{}
	ok := call("Coordinator.RequestTask", &args, &reply)
	return reply,ok
}

func closeTask(taskType int, taskId int) bool {
	args := CloseTaskArgs{}
	args.TaskType = taskType
	args.TaskId = taskId
	reply := CloseTaskReply{}
	ok := call("Coordinator.CloseTask", &args, &reply)
	return ok
}

func doMapTask(mapf func(string, string) []KeyValue, r RequestTaskReply) {
	fmt.Println("Map")
	fmt.Println(r)
	intermediate := []KeyValue{}
	// read date
	filename := r.MapFilename
	file, err := os.Open(filename)
	if err != nil {
        	log.Fatalf("cannot open %v", filename)
        }
	content, err := ioutil.ReadAll(file)
	if err != nil {
        	log.Fatalf("cannot read %v", filename)
        }
        file.Close()
	// map process
        kva := mapf(filename, string(content))
        intermediate = append(intermediate, kva...)
	
	// save result
	cache := make([][]KeyValue, r.ReduceNum)
	for i := range cache {
		cache[i] = []KeyValue{}
	}
	
	for _,kv := range intermediate {
		cache[ihash(kv.Key)%r.ReduceNum] = append(cache[ihash(kv.Key)%r.ReduceNum], kv)
	}

	for i := range cache {
		sort.Sort(ByKey(cache[i]))
		oname := fmt.Sprintf("mr-%v-%v", r.TaskId, i)
		file, err := os.Create(oname)
		if err != nil {
			panic(err)
		}
		enc := json.NewEncoder(file)
  		for _, kv := range cache[i] {
    			err := enc.Encode(&kv)
			if err != nil {
				panic(err)
			}
		}
		file.Close()
	}
	ok := closeTask(MapTask, r.TaskId)
	if !ok {
		panic("Close Map Task Failed!")
	}
}

func doReduceTask(reducef func(string, []string) string, r RequestTaskReply) {
	fmt.Println("Reduce")
	fmt.Println(r)
	time.Sleep(time.Second)
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		reply,ok := requestTask()
		if !ok {
			panic("call failed!")
		} 
		switch reply.TaskType {

			case MapTask:
				doMapTask(mapf, reply)
			case ReduceTask:
				doReduceTask(reducef, reply)
			case Wait:
				time.Sleep(time.Second)
			case Complete:
				return
			default:
				panic(fmt.Sprintf("unexpected TaskType %v", reply.TaskType))
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
	if ok {
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

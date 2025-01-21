package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
import "sync"

type TaskRecd struct {
	mu sync.Mutex
	Recd map[int]time.Time
}

func (t *TaskRecd) Size() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.Recd)
}

func (t *TaskRecd) Append(id int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.Recd[id] = time.Now()
}

type Coordinator struct {
	// Your definitions here.
	files []string
	nMap int
	nReduce int
	mapArrgRecd map[int]time.Time
	mapFinishRecd map[int]time.Time
	reduceArrgRecd map[int]time.Time
	reduceFinishRecd map[int]time.Time
	mu sync.Mutex
}

func (c *Coordinator) Init(files []string, nReduce int) {
	c.files = files
	c.nMap = len(files)
	c.nReduce = nReduce
	// c.mapArrgRecd = TaskRecd{} 
	c.mapArrgRecd = make(map[int]time.Time)
	// c.mapFinishRecd = TaskRecd{} 
	c.mapFinishRecd = make(map[int]time.Time)
	// c.reduceArrgRecd = TaskRecd{} 
	c.reduceArrgRecd = make(map[int]time.Time)
	// c.reduceFinishRecd = TaskRecd{} 
	c.reduceFinishRecd = make(map[int]time.Time)
	
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	// Map Task
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.nMap > len(c.mapArrgRecd) {
		reply.TaskType = MapTask
		reply.TaskId = len(c.mapArrgRecd)
		reply.MapFilename = c.files[reply.TaskId]
		reply.ReduceNum = c.nReduce
		c.mapArrgRecd[reply.TaskId] = time.Now()
		return nil
	}
	// Wait for all map task be completed
	if c.nMap > len(c.mapFinishRecd) {
		reply.TaskType = Wait
		return nil
	}
	// Reduce Task
	if c.nReduce > len(c.reduceArrgRecd) {
		reply.TaskType = ReduceTask
		reply.TaskId = len(c.reduceArrgRecd)
		reply.MapNum = c.nMap
		c.reduceArrgRecd[reply.TaskId] = time.Now()
		return nil
	}
	// Wait for all reduce task be completed
	if c.nReduce > len(c.reduceFinishRecd) {
		reply.TaskType = Wait
		return nil
	}
	// Quit all work
	if c.nReduce == len(c.reduceFinishRecd) {
		reply.TaskType = Complete
		return nil
	}
	// Unexcpet
	reply.TaskType = 99
	return nil
}
func (c *Coordinator) CloseTask(args *CloseTaskArgs, reply *CloseTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch args.TaskType {
		case MapTask:
			c.mapFinishRecd[args.TaskId] = time.Now()
		case ReduceTask:
			c.reduceFinishRecd[args.TaskId] = time.Now()
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

//
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
	c.Init(files, nReduce)

	c.server()
	return &c
}

package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
	"net"
	"os"
	"net/rpc"
	"net/http"
)

// mutex
var mu sync.Mutex

// 任务元信息
type TaskMetaInfo struct {
	taskAddr *Task			// 任务地址
	taskState int			// 任务状态	
	beginTime time.Time		// 开始时间
}

// MetaHolder 任务元信息持有者
type TaskMetaHolder struct {
	// 保存全部任务的元数据
	metaMap map[int]*TaskMetaInfo
}

// Coordinator结构
type Coordinator struct {
	taskMetaHolder TaskMetaHolder 	// 任务元信息持有者
	state int						// 任务状态
	mapChannel chan *Task			// Map任务channel
	reduceChannel chan *Task		// Reduce任务channel
	reduceNum int					// Reduce任务数量
	files []string					// 文件
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		taskMetaHolder: TaskMetaHolder{make(map[int]*TaskMetaInfo, len(files)+nReduce)},
		state: MapState,
		mapChannel: make(chan *Task, len(files)),
		reduceChannel: make(chan *Task, nReduce),
		reduceNum: nReduce,
		files: files}
	// 初始化map tasks
	c.MakeMapTasks(files)
	// start server and timeout checker
	c.server()
	go c.CheckTimeOut()
	return &c
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
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
	mu.Lock()
	defer mu.Unlock()
	if c.state == AllDone {
		return true
	}
	return false
}

// 初始化map tasks
func (c *Coordinator) MakeMapTasks(files []string) {
	for id, fileName := range files {
		task := Task{
			TaskId: id,
			TaskType:  MapTask,
			FileName:  []string{fileName},
			ReduceNum: c.reduceNum,
		}
		taskMetaInfo := TaskMetaInfo{taskAddr: &task, taskState: Waiting}
		c.taskMetaHolder.metaMap[id] = &taskMetaInfo
		c.mapChannel <- &task
	}
}

// 初始化reduce tasks
func (c *Coordinator) MakeReduceTasks() {
	for i := 0; i < c.reduceNum; i++ {
		id := i + len(c.files)
		task := Task{
			TaskId:   id,
			TaskType: ReduceTask,
			FileName: selectReduceFiles(i),
		}
		taskMetaInfo := TaskMetaInfo{taskAddr: &task, taskState: Waiting}
		c.taskMetaHolder.metaMap[id] = &taskMetaInfo
		c.reduceChannel <- &task
	}
}

// assignMapTask 分配 Map 任务给工作节点
func (c *Coordinator) assignMapTask(reply *Task) error {
	if len(c.mapChannel) > 0 {
		*reply = *<-c.mapChannel
		if !c.taskMetaHolder.JudgeState(reply.TaskId) {
			fmt.Printf("Map-task-id[ %d ] is running\n", reply.TaskId)
		}
	} else {
		reply.TaskType = WaitingTask
		if c.taskMetaHolder.CheckAllTasks() {
			c.ToNextState()
		}
	}
	return nil
}

// assignReduceTask 分配 Reduce 任务给工作节点
func (c *Coordinator) assignReduceTask(reply *Task) error {
	if len(c.reduceChannel) > 0 {
		*reply = *<-c.reduceChannel
		if !c.taskMetaHolder.JudgeState(reply.TaskId) {
			fmt.Printf("Reduce-task-id[ %d ] is running\n", reply.TaskId)
		}
	} else {
		reply.TaskType = WaitingTask
		if c.taskMetaHolder.CheckAllTasks() {
			c.ToNextState()
		}
	}
	return nil
}

// PullTask 为工作节点分配任务
func (c *Coordinator) PullTask(args *TaskArgs, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()

	switch c.state {
	case MapState:
		return c.assignMapTask(reply)
	case ReduceState:
		return c.assignReduceTask(reply)
	case AllDone:
		reply.TaskType = ExitTask
	}

	return nil
}

// selects files for reduce task
func selectReduceFiles(reduceNum int) []string {
	s := []string{}
	path, _ := os.Getwd()
	files, _ := ioutil.ReadDir(path)
	for _, f := range files {
		if strings.HasPrefix(f.Name(), "mr-tmp-") && strings.HasSuffix(f.Name(), strconv.Itoa(reduceNum)) {
			s = append(s, f.Name())
		}
	}
	return s
}

// judge whether all tasks are done
func (t *TaskMetaHolder) CheckAllTasks() bool {
	UnDoneNum, DoneNum := 0, 0
	for _, v := range t.metaMap {
		if v.taskState == Done {
			DoneNum++
		} else {
			UnDoneNum++
		}
	}
	return DoneNum > 0 && UnDoneNum == 0
}

// 检测一个task的状态
func (t *TaskMetaHolder) JudgeState(taskId int) bool {
	taskInfo, ok := t.metaMap[taskId]
	if !ok || taskInfo.taskState != Waiting {
		return false
	}
	taskInfo.taskState = Working
	taskInfo.beginTime = time.Now()
	return true
}

// 改变状态
func (c *Coordinator) ToNextState() {
	if c.state == MapState {
		c.MakeReduceTasks()
		c.state = ReduceState
		fmt.Println("State changed: ReduceState")
	} else if c.state == ReduceState {
		c.state = AllDone
	}
}

// 标记一个task为done
func (c *Coordinator) MarkDone(args *Task, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()
	meta, ok := c.taskMetaHolder.metaMap[args.TaskId]
	if ok && meta.taskState == Working {
		meta.taskState = Done
	}
	return nil
}

// 检测超时任务
func (c *Coordinator) CheckTimeOut() {
	for {
		time.Sleep(2 * time.Second)
		mu.Lock()
		if c.state == AllDone {
			mu.Unlock()
			break
		}
		for _, v := range c.taskMetaHolder.metaMap {
			if v.taskState == Working && time.Since(v.beginTime) > 10*time.Second {
				if v.taskAddr.TaskType == MapTask {
					v.taskState = Waiting
					c.mapChannel <- v.taskAddr
				} else if v.taskAddr.TaskType == ReduceTask {
					v.taskState = Waiting
					c.reduceChannel <- v.taskAddr
				}
			}
		}
		mu.Unlock()
	}

}

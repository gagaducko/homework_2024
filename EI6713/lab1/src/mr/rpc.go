package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
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

type Task struct {
	TaskId int			// 任务ID
	TaskType int  		// 任务状态：0-Map、1-Reduce
	ReduceNum int		// Reduce数量
	FileName []string	// 文件切片
}

type TaskArgs struct {
}

// coordinator 枚举
const (
	MapState = iota	// 映射状态
	ReduceState		// 归约状态
	AllDone			// 所有任务完成
)

// 任务状态 枚举
const (
	Working = iota	// 正在执行
	Waiting			// 等待执行
	Done			// 执行完成
)

// 常量定义任务类型
const (
	MapTask = iota	// 映射任务
	WaitingTask		// 等待任务
	ReduceTask		// 归约任务
	ExitTask		// 退出任务
)

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

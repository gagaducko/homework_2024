package mr

import (
	"log"
	"net/rpc"
	"hash/fnv"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.

// worker的主要函数
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	alive := true
	// 循环直到工作节点退出
	for alive {
		task := GetTask()
		switch task.TaskType {
		case MapTask:
			DoMapTask(&task, mapf)
			TaskDone(&task)
		case WaitingTask:
			fmt.Println("Now the TaskType is WaitingTask, please wait...")
			time.Sleep(time.Second)
		case ReduceTask:
			DoReduceTask(&task, reducef)
			TaskDone(&task)
		case ExitTask:
			fmt.Println("Now the TaskType is ExitTask, Worker exit")
			alive = false
		}
	}
}

// 执行reduce任务
func DoReduceTask(task *Task, reducef func(string, []string) string) {
	// 获取 Reduce 任务编号
	reduceNum := task.TaskId
	// 从临时文件中读取中间键值对，并对其进行排序
	intermediate := shuffle(task.FileName)
	// 设置最终输出文件名
	finalName := fmt.Sprintf("mr-out-%d", reduceNum)
	fmt.Printf("Now is reduce %d, file %v\n", reduceNum, task.FileName)
	// 创建最终输出文件
	file, err := os.Create(finalName)
	if err != nil {
		log.Fatal("create file failed:", err)
	}
	// 遍历中间键值对切片，对每个键执行 Reduce 函数并写入最终输出文件
	for i := 0; i < len(intermediate); {
		j := i + 1
		// 查找具有相同键的键值对，并将它们的值组成切片
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		// 调用用户定义的 Reduce 函数
		output := reducef(intermediate[i].Key, values)
		// 将 Reduce 函数的结果写入最终输出文件
		fmt.Fprintf(file, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	// 关闭最终输出文件
	file.Close()
}


// 执行map任务
// DoMapTask 函数执行 Map 任务
func DoMapTask(task *Task, mapf func(string, string) []KeyValue) {
	// 创建一个空的中间键值对切片，用于存储 Map 函数的输出结果
	intermediate := []KeyValue{}
	// 打开任务指定的文件
	file, err := os.Open(task.FileName[0])
	if err != nil {
		log.Fatal("cannot open %v", task.FileName[0])
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.FileName[0])
	}
	file.Close()
	// 获取 Reduce 任务数量
	reduceNum := task.ReduceNum 
	// 调用用户定义的 Map 函数处理文件内容并得到中间键值对
	intermediate = mapf(task.FileName[0], string(content)) 
	// 创建一个二维切片用于分配中间键值对到不同的 Reduce 任务
	hashKv := make([][]KeyValue, reduceNum) 
	// 将中间键值对分配到不同的 Reduce 任务
	for _, value := range intermediate {
		// 使用哈希函数将键映射到对应的 Reduce 任务
		index := ihash(value.Key) % reduceNum
		hashKv[index] = append(hashKv[index], value)
	}
	// 将分配好的中间键值对写入到临时文件中，每个 Reduce 任务对应一个临时文件
	for i := 0; i < reduceNum; i++ {
		fileName := "mr-tmp-" + strconv.Itoa(task.TaskId) + "-" + strconv.Itoa(i)
		newFile, err := os.Create(fileName)
		if err != nil {
			log.Fatal("create file failed:", err)
		}
		newEncoder := json.NewEncoder(newFile)
		for _, kv := range hashKv[i] {
			err := newEncoder.Encode(kv)
			if err != nil {
				log.Fatal("encode failed:", err)
			}
		}
		newFile.Close()
	}
}

// 排序，将reduce任务下的全部放一起
// 读取中间文件并对键值对进行排序
// shuffle 函数从文件中读取键值对并对其进行排序
func shuffle(files []string) []KeyValue {
	// 创建一个空的键值对切片，用于存储读取的键值对
	kva := []KeyValue{}
	// 遍历所有文件名
	for _, fileName := range files {
		// 打开文件
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", file)
		}
		decoder := json.NewDecoder(file)
		// 循环解码文件中的键值对
		for {
			keyValue := KeyValue{}
			err := decoder.Decode(&keyValue)
			if err != nil {
				// 如果解码结束，则退出循环
				break
			}
			// 将解码的键值对添加到切片中
			kva = append(kva, keyValue)
		}
		// 关闭文件
		file.Close()
	}
	// 对键值对切片进行排序
	sort.Sort(ByKey(kva))
	return kva // 返回排序后的键值对切片
}

// 标记任务完成
func TaskDone(task *Task) {
	args := task
	reply := Task{}
	ok := call("Coordinator.MarkDone", &args, &reply)
	if ok {
		fmt.Println("Task type is %d Task Done!", task.TaskType)
	}
}

// GetTask 从coordinator获取任务
func GetTask() Task {
	args := TaskArgs{}
	task := Task{}
	if ok := call("Coordinator.PullTask", &args, &task); ok {
	} else {
		fmt.Printf("Remote Call Failed!\n")
	}
	return task
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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

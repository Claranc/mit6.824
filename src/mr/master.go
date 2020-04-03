package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	Map     = 1
	Reduce  = 2
	Success = 3
)

type Master struct {
	// Your definitions here.
	inputFileList []string
	mu            sync.Mutex
	nReduce       int
	State         int
	MapTask       map[int]*MapTaskStatus
	ReduceTask    map[int]*ReduceTaskStatus
}

type MapTaskStatus struct {
	Status    int
	StartTime time.Time
}

type ReduceTaskStatus struct {
	Status    int
	StartTime time.Time
	fileList  []string
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


// GetMapTask 分配MAP任务
func (m *Master) GetMapTask(args *GetMapTaskRequest, reply *GetMapTaskResponse) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var fileName string
	var mapStatus int
	var mapID int
	isFinished := true
	for k, v := range m.MapTask {
		fmt.Printf("k %d, v:%+v \n", k, v.Status)
		if v.Status == UnInit {
			isFinished = false
			fileName = m.inputFileList[k]
			mapStatus = Normal
			v.Status= Processing
			v.StartTime = time.Now()
			mapID = k
			break
		} else if v.Status == Processing {
			if time.Since(v.StartTime) > 10*time.Second {
				isFinished = false
				fileName = m.inputFileList[k]
				mapStatus = Normal
				mapID = k
				v.StartTime = time.Now()
				break
			}
		}
	}
	if isFinished {
		m.State = Reduce
		mapStatus = Finish
	} else if fileName == "" {
		mapStatus = Waiting
	}
	reply.FileName = fileName
	reply.MapID = mapID
	reply.ReduceSum = m.nReduce
	reply.MapStatus = mapStatus
	fmt.Printf("res = %+v\n", reply)
	return nil
}

// Shuffle map完成，提交给master
func (m *Master) Shuffle(args *ShuffleRequest, reply *ShuffleResponse) error {
	//m.mu.Lock()
	//defer m.mu.Unlock()
	// 先查询是否为重复提交
	mapInfo, ok := m.MapTask[args.MapID]
	if ok {
		if mapInfo.Status == Finish {
			// 已完成，属于重复提交
			reply.Status = true
			return nil
		} else if mapInfo.Status == UnInit {
			// 未开始，错误状态，不匹配
			reply.Status = false
			return nil
		}
	} else {
		//未查询到该MAP信息
		reply.Status = false
		return nil
	}
	mapInfo.Status = Finish
	for k, v := range args.FileListMap {
		_, ok := m.ReduceTask[k]
		if !ok {
			m.ReduceTask[k] = &ReduceTaskStatus{
				Status:UnInit,
				fileList:args.FileListMap[k],
			}
		} else {
			m.ReduceTask[k].fileList = append(m.ReduceTask[k].fileList, v...)
		}
	}
	reply.Status = true
	return nil
}


// GetReduceTask 分配REDUCE任务
func (m *Master) GetReduceTask(args *GetReduceTaskRequest, reply *GetReduceTaskResponse) error {
	//m.mu.Lock()
	//defer m.mu.Unlock()
	if m.State == Map {
		// map阶段未完成
		reply = &GetReduceTaskResponse{
			Status: UnInit,
		}
		return nil
	}

	var reduceID int
	var fileList []string
	var reduceStatus int
	isFinished := true
	for k, v := range m.ReduceTask {
		if v.Status == UnInit {
			isFinished = false
			// 返回参数
			reduceID = k
			fileList = v.fileList
			// 内部更改
			v.StartTime = time.Now()
			v.Status = Processing
			reduceStatus = Normal
			break
		} else if v.Status == Processing {
			isFinished = false
			if time.Since(v.StartTime) > 10*time.Second {
				// 返回参数
				reduceID = k
				fileList = v.fileList
				// 内部更改
				v.StartTime = time.Now()
				reduceStatus = Normal
				break
			}
		}
	}
	if isFinished {
		reduceStatus = Finish
		m.State = Success
	} else if fileList == nil {
		reduceStatus = Waiting
	}
	reply.ReduceID = reduceID
	reply.FileList = fileList
	reply.Status = reduceStatus
	return nil
}

func (m *Master) SubmitReduce(args *SubmitReduceRequest, reply *SubmitReduceResponse) error {
	//m.mu.Lock()
	//defer m.mu.Unlock()
	m.ReduceTask[args.ReduceID].Status = Finish
	reply.Status = true
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	if m.State == Success {
		return true
	}

	// Your code here.
	if m.State == Map {
		return false
	}
	for _, v := range m.ReduceTask {
		if v.Status != Finish {
			return false
		}
	}
	m.State = Success
	return true
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.ReduceTask = make(map[int]*ReduceTaskStatus)
	m.MapTask = make(map[int]*MapTaskStatus)
	m.inputFileList = files
	m.nReduce = nReduce
	m.State = Map
	for i, _ := range files {
		m.MapTask[i] = &MapTaskStatus{
			Status:UnInit,
		}
	}

	m.server()
	return &m
}

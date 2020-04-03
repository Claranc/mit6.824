package mr

import (
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
	inputFileList []string                  //输入文件列表
	mu            sync.Mutex                //互斥锁，操作贡献变量均需要加锁
	nReduce       int                       //Reduce数量
	State         int                       //当前状态 map reduce success
	MapTask       map[int]*MapTaskStatus    //map 任务状态
	ReduceTask    map[int]*ReduceTaskStatus //reduce 任务状态
}

type MapTaskStatus struct {
	Status    int       //当前状态
	StartTime time.Time // 开始时间
}

type ReduceTaskStatus struct {
	Status    int       //当前状态
	StartTime time.Time //开始时间
	fileList  []string  //临时文件列表
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

	if m.State != Map {
		reply.MapStatus = Finish
		return nil
	}

	var fileName string
	var mapStatus int
	var mapID int
	// 轮询map任务列表，找到是否存在可分配的Map任务
	for k, v := range m.MapTask {
		if v.Status == UnInit {
			// 未开始，直接分配
			fileName = m.inputFileList[k]
			mapStatus = Normal
			v.Status = Processing
			v.StartTime = time.Now()
			mapID = k
			break
		} else if v.Status == Processing {
			if time.Since(v.StartTime) > 10*time.Second {
				// 已开始，但是超时，重新分配
				fileName = m.inputFileList[k]
				mapStatus = Normal
				mapID = k
				v.StartTime = time.Now()
				break
			}
		}
	}
	if fileName == "" {
		// 当前没有可分配的map任务，但是还有worker在执行map操作，返回继续监听
		mapStatus = Waiting
	}
	reply.FileName = fileName
	reply.MapID = mapID
	reply.ReduceSum = m.nReduce
	reply.MapStatus = mapStatus
	return nil
}

// Shuffle map完成，提交给master
func (m *Master) Shuffle(args *ShuffleRequest, reply *ShuffleResponse) error {
	m.mu.Lock()
	defer m.mu.Unlock()
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
	// 处理map提交
	mapInfo.Status = Finish
	for k, v := range args.FileListMap {
		_, ok := m.ReduceTask[k]
		if !ok {
			m.ReduceTask[k] = &ReduceTaskStatus{
				Status:   UnInit,
				fileList: args.FileListMap[k],
			}
		} else {
			m.ReduceTask[k].fileList = append(m.ReduceTask[k].fileList, v...)
		}
	}
	reply.Status = true
	// 该次提交完成后，查询所有map操作是否都结束了
	isFinished := true
	for _, v := range m.MapTask {
		if v.Status != Finish {
			isFinished = false
			break
		}
	}
	if isFinished {
		//所有map均完成提交，更改状态，进入Reduce
		m.State = Reduce
	}
	return nil
}

// GetReduceTask 分配REDUCE任务
func (m *Master) GetReduceTask(args *GetReduceTaskRequest, reply *GetReduceTaskResponse) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 如果当前不是阶段
	if m.State != Reduce {
		// map阶段未完成
		var respStatus int
		if m.State == Map {
			respStatus = UnInit
		} else {
			respStatus = Finish
		}
		reply = &GetReduceTaskResponse{
			Status: respStatus,
		}
		return nil
	}

	var reduceID int
	var fileList []string
	var reduceStatus int
	for k, v := range m.ReduceTask {
		if v.Status == UnInit {
			// 返回参数
			reduceID = k
			fileList = v.fileList
			// 内部更改
			v.StartTime = time.Now()
			v.Status = Processing
			reduceStatus = Normal
			break
		} else if v.Status == Processing {
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
	if fileList == nil {
		// 未分配Reduce任务，也未完成，继续等待别的worker提交，让它继续监听
		reduceStatus = Waiting
	}
	reply.ReduceID = reduceID
	reply.FileList = fileList
	reply.Status = reduceStatus
	return nil
}

func (m *Master) SubmitReduce(args *SubmitReduceRequest, reply *SubmitReduceResponse) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ReduceTask[args.ReduceID].Status = Finish

	// 查看Reduce任务是否已全部完成
	isFinished := true
	for _, v := range m.ReduceTask {
		if v.Status != Finish {
			isFinished = false
			break
		}
	}
	if isFinished {
		m.State = Success
	}
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
			Status: UnInit,
		}
	}

	m.server()
	return &m
}

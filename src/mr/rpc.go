package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X      int
	Method string
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// master维护任务状态
const (
	UnInit     = 0 // 当前step未开始
	Processing = 1 // 当前step进行中
	Finish     = 2 // 当前step已完成
	Normal     = 3 // 正常获取task
	Waiting    = 4 // 未成功获取task，需等待其他worker完成任务
)

type ShuffleRequest struct {
	MapID          int
	FileListMap    map[int][]string
}

type ShuffleResponse struct {
	Status bool
}

type GetMapTaskRequest struct{}

type GetMapTaskResponse struct {
	MapID     int
	FileName  string
	ReduceSum int
	MapStatus int
}

type GetReduceTaskRequest struct{}

type GetReduceTaskResponse struct {
	ReduceID int
	FileList []string
	Status   int
}

type SubmitReduceRequest struct {
	ReduceID int
}

type SubmitReduceResponse struct {
	Status bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

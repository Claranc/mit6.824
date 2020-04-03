package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
	"math/rand"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//用来排序
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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.

	//step: MAP
	for {
		mapTask, ok := CallGetMapTask()
		if !ok {
			log.Fatalf("rpc failed")
		}
		if mapTask.MapStatus == Finish {
			// map阶段已完成，跳出
			break
		} else if mapTask.MapStatus == Waiting {
			// 等待别的worker完成map,继续监听，防止worker宕机
			time.Sleep(2 * time.Second)
			continue
		} else if mapTask.MapStatus != Normal {
			// 非正常状态，跳出吧，不太可能有这个返回
			break
		}
		// 处理master返回的Input 文件
		mapFile, err := os.Open(mapTask.FileName)
		if err != nil {
			log.Fatalf("cannot open %v", mapTask.FileName)
		}
		content, err := ioutil.ReadAll(mapFile)
		if err != nil {
			log.Fatalf("cannot read %v", mapTask.FileName)
		}
		mapFile.Close()
		kva := mapf(mapTask.FileName, string(content))

		fileListMap := make(map[int][]string) //key: reduceID  value：该reduceID的文件列表
		fileNameMap := make(map[string][]KeyValue) // key: 文件名  value: kv键值对
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		randNo := r.Intn(1000) // 每个worker在生成map阶段临时文件的时候，文件名带有一个随机数，防止不同worker在针对同一map任务进行写冲突
		for _, kv := range kva {
			idx := ihash(kv.Key)%mapTask.ReduceSum //为该key找到对应的reduceID
			reduceFilename := buildReduceFilename(randNo, mapTask.MapID, idx) //生成临时文件名
			if _, ok := fileNameMap[reduceFilename]; !ok {
				//该文件第一次出现，初始化
				if _, ok := fileListMap[idx]; !ok {
					fileListMap[idx] = []string{reduceFilename}
				} else {
					fileListMap[idx] = append(fileListMap[idx], reduceFilename)
				}
				fileNameMap[reduceFilename] = []KeyValue{kv}
				continue
			}
			//该文件名已生成，进行内容追加
			fileNameMap[reduceFilename] = append(fileNameMap[reduceFilename], kv)
		}
		// 将文件内容统一落盘
		for filename,kvs := range fileNameMap {
			file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND,0644)
			if err != nil {
				log.Fatalf("openfile failed, err:%+v", err)
			}
			enc := json.NewEncoder(file)
			for _, kv := range kvs {
				err = enc.Encode(&kv)
				if err != nil {
					log.Fatalf("write reduce file failed, err %v", err)
				}
			}
			file.Close()
		}


		// shuffle  通知master 该map操作已完成以及告知生成的临时文件
		ok = CallShuffle(mapTask.MapID, fileListMap)
		if !ok {
			log.Fatalf("Shuffle failed")
		}
	}

	//step: REDUCE
	for {
		reduceTask, ok := CallGetReduceTask()
		if !ok {
			// RPC操作失败，默认master崩了，结束
			break
		}
		if reduceTask.Status == Finish {
			// 该任务Reduce操作已完成，结束
			break
		} else if reduceTask.Status == Waiting || reduceTask.Status == UnInit {
			// 等待别的worker进行Reduce操作进行监听； Reduce操作未开始，继续监听
			time.Sleep(2 * time.Second)
			continue
		}
		// 读取临时文件，进行Reduce操作
		res := []KeyValue{}
		for _, filename := range reduceTask.FileList {
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("open file failed, err:%+v", err)
			}
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				res = append(res, kv)
			}
			file.Close()
		}
		sort.Sort(ByKey(res))
		oFileName := "mr-out-" + strconv.Itoa(reduceTask.ReduceID)
		ofile, _ := os.Create(oFileName)
		i := 0
		for i < len(res) {
			j := i + 1
			for j < len(res) && res[j].Key == res[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, res[k].Value)
			}
			output := reducef(res[i].Key, values)

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(ofile, "%v %v\n", res[i].Key, output)
			i = j
		}
		ofile.Close()
		ok = CallSubmitReduce(reduceTask.ReduceID)
		if !ok {
			log.Fatalf("submit reduce failed")
		}
	}
}

func buildReduceFilename(randNum int, mapID int, reduceID int) string {
	return "mr_" + strconv.Itoa(randNum) + "_" + strconv.Itoa(mapID) + "_" + strconv.Itoa(reduceID) + ".txt"
}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

func CallGetMapTask() (*GetMapTaskResponse, bool) {
	args := &GetMapTaskRequest{}
	reply := &GetMapTaskResponse{}
	flag := call("Master.GetMapTask", &args, &reply)
	return reply, flag
}

func CallGetReduceTask() (*GetReduceTaskResponse, bool) {
	args := &GetReduceTaskRequest{}
	reply := &GetReduceTaskResponse{}
	flag := call("Master.GetReduceTask", &args, &reply)
	return reply, flag
}

func CallShuffle(mapID int, reduceFile map[int][]string) bool {
	args := &ShuffleRequest{
		MapID:          mapID,
		FileListMap:       reduceFile,
	}
	reply := &ShuffleResponse{}
	flag := call("Master.Shuffle", &args, &reply)
	return flag
}

func CallSubmitReduce(reduceID int) bool {
	args := &SubmitReduceRequest{ReduceID: reduceID}
	reply := &SubmitReduceResponse{}
	flag := call("Master.SubmitReduce", &args, &reply)
	return flag
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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

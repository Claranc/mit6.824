package mr

import (
	"code.byted.org/gopkg/logs"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
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
			time.Sleep(2 * time.Second)
			continue
		}
		if mapTask.MapStatus == Finish {
			break
		} else if mapTask.MapStatus == Waiting {
			time.Sleep(2 * time.Second)
			continue
		}
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

		fileListMap := make(map[int][]string)
		fileNameMap := make(map[string][]KeyValue)
		for _, kv := range kva {
			idx := ihash(kv.Key)%mapTask.ReduceSum
			reduceFilename := buildReduceFilename(mapTask.MapID, idx)
			if _, ok := fileNameMap[reduceFilename]; !ok {
				if _, ok := fileListMap[idx]; !ok {
					fileListMap[idx] = []string{reduceFilename}
				} else {
					fileListMap[idx] = append(fileListMap[idx], reduceFilename)
				}
				fileNameMap[reduceFilename] = []KeyValue{kv}
				continue
			}
			fileNameMap[reduceFilename] = append(fileNameMap[reduceFilename], kv)
		}
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


		// shuffle
		ok = CallShuffle(mapTask.MapID, fileListMap)
		if !ok {
			logs.Fatalf("Shuffle failed")
		}
	}

	//step: REDUCE
	for {
		reduceTask, ok := CallGetReduceTask()
		if !ok {
			time.Sleep(2 * time.Second)
			continue
		}
		if reduceTask.Status == Finish {
			break
		} else if reduceTask.Status == Waiting || reduceTask.Status == UnInit {
			time.Sleep(2 * time.Second)
			continue
		}
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

func buildReduceFilename(mapID int, reduceID int) string {
	return "mr_" + strconv.Itoa(mapID) + "_" + strconv.Itoa(reduceID) + ".txt"
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
	fmt.Printf("CallGetMapTask reply %+v\n", reply)
	return reply, flag
}

func CallGetReduceTask() (*GetReduceTaskResponse, bool) {
	args := &GetReduceTaskRequest{}
	reply := &GetReduceTaskResponse{}
	flag := call("Master.GetReduceTask", &args, &reply)
	fmt.Printf("CallGetReduceTask reply %+v\n", reply)
	return reply, flag
}

func CallShuffle(mapID int, reduceFile map[int][]string) bool {
	args := &ShuffleRequest{
		MapID:          mapID,
		FileListMap:       reduceFile,
	}
	reply := &ShuffleResponse{}
	flag := call("Master.Shuffle", &args, &reply)
	fmt.Printf("CallShuffle reply %+v\n", reply)
	return flag
}

func CallSubmitReduce(reduceID int) bool {
	args := &SubmitReduceRequest{ReduceID: reduceID}
	reply := &SubmitReduceResponse{}
	flag := call("Master.SubmitReduce", &args, &reply)
	fmt.Printf("CallSubmitReduce reply %+v\n", reply)
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

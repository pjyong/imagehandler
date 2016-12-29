package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"sync"
	"time"
)

// 任务列表
var datastore map[int]Task
var datastoreMutex sync.RWMutex

// 标记最后未完成的任务ID，其实就是少一些遍历
var oldestUnfinishedTask int
var oNFTMutex sync.RWMutex

// 0=>还没开始
// 1=>正在处理中
// 2=>完成的
type Task struct {
	Id    int `json:"id"`
	State int `json:"state"`
}

func main() {
	if !registerInKVStore() {
		return
	}
	datastore = make(map[int]Task)

	// 任务相关api
	http.HandleFunc("/newtask", NewTask)
	http.HandleFunc("/gettask", GetTask)
	http.HandleFunc("/listtask", ListTask)
	http.HandleFunc("/settask", SetTask)
	// 发送任务
	http.HandleFunc("/sendtask", SendTask)

	http.ListenAndServe(":3001", nil)
}

func ListTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:非法请求")
		return
	}
	datastoreMutex.RLock()
	for key, value := range datastore {
		fmt.Fprintln(w, key, ": ", "id:", value.Id, " state:", value.State)
	}
	datastoreMutex.RUnlock()
}

func NewTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:非法请求")
		return
	}
	datastoreMutex.Lock()
	taskToAdd := Task{
		len(datastore), 0,
	}
	datastore[taskToAdd.Id] = taskToAdd
	datastoreMutex.Unlock()
	fmt.Fprint(w, taskToAdd.Id)
}

func GetTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:非法请求")
		return
	}
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		fmt.Fprint(w, err)
		return
	}
	if len(values.Get("id")) == 0 {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "错误参数")
		return
	}

	id, err := strconv.Atoi(string(values.Get("id")))
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "错误参数")
		return
	}

	datastoreMutex.RLock()
	bIsInError := err != nil || id >= len(datastore)
	datastoreMutex.RUnlock()

	if bIsInError {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Wrong input")
		return
	}
	datastoreMutex.RLock()
	value := datastore[id]
	datastoreMutex.RUnlock()

	response, err := json.Marshal(value)

	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, err)
		return
	}

	fmt.Fprint(w, string(response))
}

func SetTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:非法请求")
		return
	}
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:", err)
		return
	}
	if len(values.Get("id")) == 0 {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:参数错误")
		return
	}
	if len(values.Get("state")) == 0 {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:参数错误")
		return
	}
	id, err := strconv.Atoi(string(values.Get("id")))
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "错误参数")
		return
	}
	state, err := strconv.Atoi(string(values.Get("state")))
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "错误参数")
		return
	}
	// 检查下参数是否ok

	bErrored := false
	bErroredMsg := "错误参数"
	datastoreMutex.Lock()
	if id >= len(datastore) || state < 0 || state > 2 {
		bErrored = true
	} else {
		// 如果要将任务设置成完成，则必须要求它是正在进行中
		if state == 2 && datastore[id].State != 1 {
			bErrored = true
			bErroredMsg = "只有在进行中的任务才能设置成完成"
		} else {
			datastore[id] = Task{
				id, state,
			}
		}
	}
	datastoreMutex.Unlock()

	if bErrored {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, bErroredMsg)
		return
	}
	fmt.Fprint(w, "success")
}

func SendTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:非法请求")
		return
	}
	bErrored := false
	datastoreMutex.RLock()
	if len(datastore) == 0 {
		bErrored = true
	}
	datastoreMutex.RUnlock()

	if bErrored {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:没有任务需要发送啊")
		return
	}

	taskToSend := Task{Id: -1, State: 0}
	// 从没有完成的任务ID开始循环
	oNFTMutex.Lock()
	datastoreMutex.Lock()
	for i := oldestUnfinishedTask; i < len(datastore); i++ {
		if datastore[oldestUnfinishedTask].State == 2 {
			oldestUnfinishedTask++
			continue
		}
		if datastore[i].State == 0 {
			datastore[i] = Task{Id: i, State: 1}
			taskToSend = datastore[i]
			break
		}
	}
	datastoreMutex.Unlock()
	oNFTMutex.Unlock()

	if taskToSend.Id == -1 {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:没有任务需要发送啊")
		return
	}

	myTaskID := taskToSend.Id
	// 开启一个go runtine，过两分钟之后来检查这个任务完成了没有，没有的话重新将其标记为0
	go func() {
		time.Sleep(time.Second * 120)
		datastoreMutex.Lock()
		if datastore[myTaskID].State == 1 {
			datastore[myTaskID] = Task{Id: myTaskID, State: 0}
		}
		datastoreMutex.Unlock()
	}()

	response, err := json.Marshal(taskToSend)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, err)
		return
	}
	fmt.Fprint(w, string(response))
}

func registerInKVStore() bool {
	if len(os.Args) < 3 {
		fmt.Println("Error: 参数太少了.")
		return false
	}
	databaseAddress := os.Args[1] // The address of itself
	keyValueStoreAddress := os.Args[2]

	response, err := http.Post("http://"+keyValueStoreAddress+"/set?key=databaseAddress&value="+databaseAddress, "", nil)
	if err != nil {
		fmt.Println(err)
		return false
	}
	data, err := ioutil.ReadAll(response.Body)
	if err != nil {
		fmt.Println(err)
		return false
	}
	if response.StatusCode != http.StatusOK {
		fmt.Println("Error: 连接键值数据库失败: ", string(data))
		return false
	}
	return true
}

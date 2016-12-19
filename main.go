package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"sync"
)

var kvStorage map[string]string
var kVStoreMutex sync.RWMutex

// 任务列表
var datastore map[int]Task
var datastoreMutex sync.RWMutex

type Task struct {
	Id    int `json:"id"`
	State int `json:"state"`
}

func main() {
	kvStorage = make(map[string]string)
	kVStoreMutex = sync.RWMutex{}
	// 创建key-value相关api
	http.HandleFunc("/get", get)
	http.HandleFunc("/set", set)
	http.HandleFunc("/remove", remove)
	http.HandleFunc("/list", list)
	// 任务相关api
	http.HandleFunc("/newtask", NewTask)
	http.HandleFunc("/gettask", GetTask)
	http.HandleFunc("/listtasks", ListTask)

	http.ListenAndServe(":3000", nil)
}

func get(w http.ResponseWriter, r *http.Request) {
	// 需要检查是不是Get请求
	if r.Method != "GET" {
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
	if len(values.Get("key")) == 0 {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:参数错误")
		return
	}
	// 设置锁
	kVStoreMutex.RLock()
	value := kvStorage[string(values.Get("key"))]
	kVStoreMutex.RUnlock()
	fmt.Fprint(w, value)
}

func set(w http.ResponseWriter, r *http.Request) {
	// 需要检查是不是Post请求
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
	if len(values.Get("key")) == 0 {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:参数错误")
		return
	}
	if len(values.Get("value")) == 0 {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:参数错误")
		return
	}
	// 设置锁
	kVStoreMutex.RLock()
	kvStorage[string(values.Get("key"))] = string(values.Get("value"))
	kVStoreMutex.RUnlock()
	fmt.Fprint(w, "success")
}

func remove(w http.ResponseWriter, r *http.Request) {
	if r.Method != "DELETE" {
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
	if len(values.Get("key")) == 0 {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:参数错误")
		return
	}
	kVStoreMutex.RLock()
	delete(kvStorage, values.Get("key"))
	kVStoreMutex.RUnlock()
}

func list(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:非法请求")
		return
	}
	kVStoreMutex.RLock()
	for key, value := range kvStorage {
		fmt.Fprintln(w, key, ":", value)
	}
	kVStoreMutex.RUnlock()
}

func ListTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:非法请求")
		return
	}
	kVStoreMutex.RLock()
	for key, value := range datastore {
		fmt.Fprintln(w, key, ": ", "id:", value.Id, " state:", value.State)
	}
	kVStoreMutex.RUnlock()
}
func NewTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:非法请求")
		return
	}
	kVStoreMutex.Lock()
	taskToAdd := Task{
		len(datastore), 0,
	}
	datastore[taskToAdd.Id] = taskToAdd
	kVStoreMutex.Unlock()
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

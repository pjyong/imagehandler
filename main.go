package main

import (
	"fmt"
	"net/http"
	"net/url"
	"sync"
)

var kvStorage map[string]string
var kVStoreMutex sync.RWMutex

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

function ListTask(w http.ResponseWriter, r *http.Request){
	
}
function NewTask(w http.ResponseWriter, r *http.Request){
	
}
function GetTask(w http.ResponseWriter, r *http.Request){
	
}

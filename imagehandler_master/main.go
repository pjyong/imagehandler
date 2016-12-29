package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
)

type Task struct {
	Id    int `json:"id"`
	State int `json:"state"`
}

var databaseLocation string
var storageLocation string
var keyValueStoreAddress string

func main() {
	if !registerInKVStore() {
		return
	}
	keyValueStoreAddress = os.Args[2]
	response, err := http.Get("http://" + keyValueStoreAddress + "/get?key=databaseAddress")
	if response.StatusCode != http.StatusOK {
		fmt.Println("Error: 不能获取数据库地址额")
		fmt.Println(response.Body)
		return
	}
	data, err := ioutil.ReadAll(response.Body)
	if err != nil {
		fmt.Println(err)
		return
	}
	databaseLocation = string(data)

	response, err = http.Get("http://" + keyValueStoreAddress + "/get?key=filesystemAddress")
	if response.StatusCode != http.StatusOK {
		fmt.Println("Error: can't get database address.")
		fmt.Println(response.Body)
		return
	}
	data, err = ioutil.ReadAll(response.Body)
	if err != nil {
		fmt.Println(err)
		return
	}
	storageLocation = string(data)

	http.HandleFunc("/new", newImage)
	http.HandleFunc("/get", getImage)
	http.HandleFunc("/isready", isReady)
	// 下面两个方法就是给worker用的，也可以让worker直接跟数据库打交道，但是没这种拓展性高
	http.HandleFunc("/getnewtask", getNewTask)
	http.HandleFunc("/settaskfinished", setTaskFinished)
	http.ListenAndServe(":3003", nil)
}

func newImage(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:非法请求")
		return
	}
	response, err := http.Post("http://"+databaseLocation+"/newtask", "text/plain", nil)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:", err)
		return
	}
	id, err := ioutil.ReadAll(response.Body)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("http://" + storageLocation + "/receiveimage?id=" + string(id) + "&state=working")
	t, err := http.Post("http://"+storageLocation+"/receiveimage?id="+string(id)+"&state=working", "image", r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:", err)
		return
	}
	fmt.Println(t)
	fmt.Fprint(w, string(id))

}

func getImage(w http.ResponseWriter, r *http.Request) {
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
		fmt.Fprint(w, "Wrong input")
		return
	}
	response, err := http.Get("http://" + storageLocation + "/serveimage?id=" + values.Get("id") + "&state=finished")
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:", err)
		return
	}

	_, err = io.Copy(w, response.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:", err)
		return
	}
}

func isReady(w http.ResponseWriter, r *http.Request) {
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
		fmt.Fprint(w, "Wrong input")
		return
	}
	// 获取任务详情
	response, err := http.Get("http://" + databaseLocation + "/gettask?id=" + values.Get("id"))
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:", err)
		return
	}
	data, err := ioutil.ReadAll(response.Body)
	if err != nil {
		fmt.Println(err)
		return
	}
	myTask := Task{}
	json.Unmarshal(data, &myTask)

	if myTask.State == 2 {
		fmt.Fprint(w, "1")
	} else {
		fmt.Fprint(w, "0")
	}
}

func getNewTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:非法请求")
		return
	}
	response, err := http.Post("http://"+databaseLocation+"/sendtask", "text/plain", nil)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:", err)
		return
	}

	_, err = io.Copy(w, response.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:", err)
		return
	}
}

func setTaskFinished(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
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
		fmt.Fprint(w, "Wrong input")
		return
	}

	response, err := http.Post("http://"+databaseLocation+"/settask?id="+values.Get("id")+"&state=2", "test/plain", nil)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:", err)
		return
	}

	_, err = io.Copy(w, response.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:", err)
		return
	}
}

// 在键值数据库中注册自己这项服务
func registerInKVStore() bool {
	if len(os.Args) < 3 {
		fmt.Println("Error:参数太少")
		return false
	}

	masterAddress := os.Args[1]
	keyValueStoreAddress := os.Args[2]

	response, err := http.Post("http://"+keyValueStoreAddress+"/set?key=masterAddress&value="+masterAddress, "", nil)
	if err != nil {
		fmt.Println(err)
		return false
	}
	if response.StatusCode != http.StatusOK {
		fmt.Println("不能连接到数据库啊")
		return false
	}
	_, err = ioutil.ReadAll(response.Body)
	if err != nil {
		fmt.Println(err)
		return false
	}
	return true
}

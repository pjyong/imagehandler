package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strconv"
)

func main() {
	if !registerInKVStore() {
		return
	}
	// 图片文件相关
	http.HandleFunc("/receiveimage", ReceiveImage)
	http.HandleFunc("/serveimage", ServeImage)
	http.ListenAndServe(":3002", nil)
}

// 保存图片
func ReceiveImage(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:非法请求")
		return
	}
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:非法请求")
		return
	}
	if len(values.Get("id")) == 0 {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:", "ID参数错误")
		return
	}
	if values.Get("state") != "working" && values.Get("state") != "finished" {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:", "state参数错误")
		return
	}
	// 检查这个id是不是一个整型
	_, err = strconv.Atoi(values.Get("id"))
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:", err)
		return
	}
	fmt.Println(values.Get("id"))
	file, err := os.Create("/home/june/tmp/" + values.Get("state") + "/" + values.Get("id") + ".png")
	defer file.Close()
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:", err)
		return
	}
	_, err = io.Copy(file, r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:", err)
		return
	}
	fmt.Fprint(w, "success")
}

func ServeImage(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:非法请求")
		return
	}
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:非法请求")
		return
	}
	if len(values.Get("id")) == 0 {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:", "ID参数错误")
		return
	}
	if values.Get("state") != "working" && values.Get("state") != "finished" {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:", "state参数错误")
		return
	}
	// 检查这个id是不是一个整型
	_, err = strconv.Atoi(values.Get("id"))
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:", err)
		return
	}
	file, err := os.Create("/home/june/tmp/" + values.Get("state") + "/" + values.Get("id") + ".png")
	defer file.Close()
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:", err)
		return
	}
	_, err = io.Copy(w, file)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:", err)
		return
	}
}

//
func registerInKVStore() bool {
	if len(os.Args) < 3 {
		fmt.Println("Error:参数太少")
		return false
	}

	filesystemAddress := os.Args[1]
	keyValueStoreAddress := os.Args[2]

	response, err := http.Post("http://"+keyValueStoreAddress+"/set?key=filesystemAddress&value="+filesystemAddress, "", nil)
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

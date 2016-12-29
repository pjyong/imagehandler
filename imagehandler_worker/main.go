package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"image"
	"image/color"
	"image/png"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

type Task struct {
	Id    int `json:"id"`
	State int `json:"state"`
}

var masterLocation string
var storageLocation string
var keyValueStoreAddress string

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Error: Too few arguments.")
		return
	}
	keyValueStoreAddress = os.Args[1]

	response, err := http.Get("http://" + keyValueStoreAddress + "/get?key=masterAddress")
	if response.StatusCode != http.StatusOK {
		fmt.Println("Error: 不能获取master服务地址")
		fmt.Println(response.Body)
		return
	}
	data, err := ioutil.ReadAll(response.Body)
	if err != nil {
		fmt.Println(err)
		return
	}
	masterLocation = string(data)
	if len(masterLocation) == 0 {
		fmt.Println("Error: master服务地址长度为0")
		return
	}

	response, err = http.Get("http://" + keyValueStoreAddress + "/get?key=filesystemAddress")
	if response.StatusCode != http.StatusOK {
		fmt.Println("Error: 不能获取文件服务")
		fmt.Println(response.Body)
		return
	}
	data, err = ioutil.ReadAll(response.Body)
	if err != nil {
		fmt.Println(err)
		return
	}
	storageLocation = string(data)
	if len(storageLocation) == 0 {
		fmt.Println("Error: can't get storage address. Length is zero.")
		return
	}

	threadCount, err := strconv.Atoi(os.Args[2])
	if err != nil {
		fmt.Println("Error: Couldn't parse thread count.")
		return
	}

	myWG := sync.WaitGroup{}
	myWG.Add(threadCount)
	for i := 0; i < threadCount; i++ {
		go func() {
			for {
				myTask, err := getNewTask(masterLocation)
				if err != nil {
					fmt.Println("Waiting 2 second timeout...")
					time.Sleep(time.Second * 2)
					continue
				}

				myImage, err := getImageFromStorage(storageLocation, myTask)
				if err != nil {
					// 如果获取不到图片，那么将任务设置成完成
					registerFinishedTask(masterLocation, myTask)
					fmt.Println("Waiting 2 second timeout...")
					time.Sleep(time.Second * 2)
					continue
				}

				myImage = doWorkOnImage(myImage)

				err = sendImageToStorage(storageLocation, myTask, myImage)
				if err != nil {
					fmt.Println(err)
					fmt.Println("Waiting 2 second timeout...")
					time.Sleep(time.Second * 2)
					continue
				}

				err = registerFinishedTask(masterLocation, myTask)
				if err != nil {
					fmt.Println(err)
					fmt.Println("Waiting 2 second timeout...")
					time.Sleep(time.Second * 2)
					continue
				}
			}
		}()
	}
	myWG.Wait()
}

func getNewTask(masterAddress string) (Task, error) {
	response, err := http.Post("http://"+masterAddress+"/getnewtask", "text/plain", nil)
	if err != nil || response.StatusCode != http.StatusOK {
		return Task{-1, -1}, err
	}
	data, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return Task{-1, -1}, err
	}

	myTask := Task{}
	err = json.Unmarshal(data, &myTask)
	if err != nil {
		return Task{-1, -1}, err
	}

	return myTask, nil
}
func getImageFromStorage(storageAddress string, myTask Task) (image.Image, error) {
	response, err := http.Get("http://" + storageAddress + "/serveimage?state=working&id=" + strconv.Itoa(myTask.Id))
	if err != nil || response.StatusCode != http.StatusOK {
		return nil, err
	}

	myImage, err := png.Decode(response.Body)
	if err != nil {
		return nil, err
	}

	return myImage, nil
}
func doWorkOnImage(myImage image.Image) image.Image {
	myCanvas := image.NewRGBA(myImage.Bounds())

	for i := 0; i < myCanvas.Rect.Max.X; i++ {
		for j := 0; j < myCanvas.Rect.Max.Y; j++ {
			r, g, b, _ := myImage.At(i, j).RGBA()
			myColor := new(color.RGBA)
			myColor.R = uint8(g)
			myColor.G = uint8(r)
			myColor.B = uint8(b)
			myColor.A = uint8(255)
			myCanvas.Set(i, j, myColor)
		}
	}

	return myCanvas.SubImage(myImage.Bounds())
}
func sendImageToStorage(storageAddress string, myTask Task, myImage image.Image) error {
	data := []byte{}
	buffer := bytes.NewBuffer(data)
	err := png.Encode(buffer, myImage)
	if err != nil {
		return err
	}
	response, err := http.Post("http://"+storageAddress+"/receiveimage?state=finished&id="+strconv.Itoa(myTask.Id), "image/png", buffer)
	if err != nil || response.StatusCode != http.StatusOK {
		return err
	}

	return nil
}

func registerFinishedTask(masterAddress string, myTask Task) error {
	response, err := http.Post("http://"+masterAddress+"/settaskfinished?id="+strconv.Itoa(myTask.Id), "test/plain", nil)
	if err != nil || response.StatusCode != http.StatusOK {
		return err
	}

	return nil
}

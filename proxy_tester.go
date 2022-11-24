package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sync"
)

type Config struct {
	Apps []App
}

type App struct {
	Name    string
	Ports   []int
	Targets []string
}

func test_proxy(proxy_host string, port int, wg *sync.WaitGroup) {
	connection, err := net.Dial("tcp", proxy_host+":"+fmt.Sprintf("%d", port))
	if err != nil {
		fmt.Println("failed to connect to", proxy_host, port, "exiting...", err)
		os.Exit(1)
	}

	write_buf := []byte("hello world!")
	read_buf := make([]byte, 12)

	for i := 0; i < 1000; i++ {
		_, err := connection.Write(write_buf)
		if err != nil {
			fmt.Println("failed to write to tcp socket", proxy_host, port, "exiting...", err)
			os.Exit(1)
		}

		_, err = connection.Read(read_buf)
		if err != nil {
			fmt.Println("failed to read from tcp socket", proxy_host, port, "exiting...", err)
			os.Exit(1)
		}

		if bytes.Compare(write_buf, read_buf) != 0 && bytes.Compare([]byte("HELLO WORLD!"), read_buf) != 0 {
			fmt.Println("failed to read echoed message", proxy_host, port, "exiting...")
			fmt.Println(read_buf, write_buf)
			os.Exit(1)
		}
	}

	fmt.Println("successfully communicated with", proxy_host, port)
	wg.Done()
}

func main() {
	if len(os.Args) != 2 {
		// no hostname provided for the proxy
		fmt.Println("usage: proxy_tester <hostname>")
		return
	}

	proxy_host := os.Args[1]

	config_data, err := os.ReadFile("./config.json")
	if err != nil {
		fmt.Println(err)
		return
	}

	var config Config
	json.Unmarshal(config_data, &config)
	fmt.Println(config)

	var wg sync.WaitGroup
	for _, app := range config.Apps {
		fmt.Println("spawning test workers for proxy: ", app.Name)
		for _, app_port := range app.Ports {
			for j := 0; j < 10; j++ {
				wg.Add(1)
				go test_proxy(proxy_host, app_port, &wg)
			}
		}
	}

	wg.Wait()

	fmt.Println(proxy_host)
}

package main

import (
	"fmt"
	"github.com/senarukana/dldb/client"
)

// var running bool = false

func start() {
	fmt.Println("welcome to dldb shell >>>")
	localAddr := "localhost:9093"
	c := client.InitDldbClient(localAddr)
	for {
		if err := c.Execute(); err != nil {
			fmt.Println("Oops, %v", err)
		}
	}
}

func main() {
	start()
}

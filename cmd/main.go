package main

import (
	"context"
	"fmt"
	"time"

	kad "github.com/FelipeRosa/go-kademlia"
	"go.uber.org/zap"
)

func main() {
	logger, _ := zap.NewProduction()

	id := kad.NewID()

	var hosts []kad.Host
	for i := 0; i < 40; i++ {
		h, err := kad.NewHost("0.0.0.0:", kad.WithLogger(logger))
		if err != nil {
			panic(err)
		}
		fmt.Println(h.ID(), h.ID().DistanceTo(id), h.LocalAddrInfo())
		hosts = append(hosts, h)
	}
	defer func() {
		for _, host := range hosts {
			host.Close()
		}
	}()

	start := time.Now()
	for i := 1; i < len(hosts); i++ {
		bootstrap := hosts[i-1]
		host := hosts[i]
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
		if err := host.Bootstrap(ctx, bootstrap.LocalAddrInfo()); err != nil {
			cancel()
			panic(err)
		}
		cancel()
	}
	fmt.Println("bootstrapping took:", time.Since(start))

	fmt.Println()
	fmt.Println("FIND_NODE")

	start = time.Now()
	fmt.Println(hosts[0].FindNode(context.Background(), id))
	fmt.Println("find node took:", time.Since(start))
}

package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"time"

	kad "github.com/FelipeRosa/go-kademlia"
	"go.uber.org/zap"
)

func main() {
	logLevel := zap.LevelFlag("log.level", zap.InfoLevel, "Sets the logger level")
	flag.Parse()

	loggerCfg := zap.NewDevelopmentConfig()
	loggerCfg.Level.SetLevel(*logLevel)
	logger, _ := loggerCfg.Build()

	var hosts []kad.Host
	for i := 0; i < 400; i++ {
		h, err := kad.NewHost("0.0.0.0:", kad.WithLogger(logger))
		if err != nil {
			panic(err)
		}
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

	for i, h := range hosts {
		key := fmt.Sprintf("test_%d", i)

		value := make([]byte, 5)
		rand.Read(value)

		start = time.Now()
		fmt.Println("storing:", value)
		if err := h.Store(context.Background(), key, value); err != nil {
			panic(err)
		}
		fmt.Println("STORE took:", time.Since(start))

		start = time.Now()
		valueFound, found, err := h.FindValue(context.Background(), key)
		if err != nil {
			panic(err)
		}
		if !found {
			panic("value not found")
		}
		fmt.Println("FIND_VALUE took:", time.Since(start))

		if !bytes.Equal(value, valueFound) {
			panic("wrong value returned")
		}
	}
}

package main

import (
	"context"
	"fmt"
	goroutine_pool "github.com/golang-infrastructure/go-goroutine-pool"
	"strconv"
	"time"
)

func readFileLines() []string {
	lines := make([]string, 10)
	for index := range lines {
		lines[index] = strconv.Itoa(index)
	}
	return lines
}

func main() {

	processGoroutine := goroutine_pool.NewGoroutinePoolWithDefaultOptions()
	processGoroutine.Options.SetMaxConsumerNum(1)

	for _, line := range readFileLines() {
		localLine := line
		_ = processGoroutine.SubmitTaskByFunc(context.Background(), func(ctx context.Context, pool *goroutine_pool.GoroutinePool, worker *goroutine_pool.Consumer) error {
			fmt.Println(localLine)
			time.Sleep(time.Second)
			return nil
		})
	}
	processGoroutine.ShutdownAndAwait()

}

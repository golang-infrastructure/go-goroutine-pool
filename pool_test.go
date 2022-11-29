package goroutine_pool

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func Test_SinglePool(t *testing.T) {

	// 单个池子的运行
	options := NewCreateGoroutinePoolOptions()
	pool := NewGoroutinePool(options)

	for i := 0; i < 100; i++ {
		err := pool.SubmitTaskByFunc(context.Background(), func(ctx context.Context, pool *GoroutinePool, worker *Consumer) error {

			time.Sleep(time.Second * 5)
			fmt.Println(time.Now())

			return nil
		})
		if err != nil {
			fmt.Println(err.Error())
			return
		}
	}

	pool.ShutdownAndAwait()
	fmt.Println("池子任务执行完成")

}

func Test_DAGPool(t *testing.T) {
	// TODO DAG任务的构造
}

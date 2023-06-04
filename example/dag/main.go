package main

import (
	"context"
	"fmt"
	goroutine_pool "github.com/golang-infrastructure/go-goroutine-pool"
	"sync/atomic"
)

func main() {

	// pool-A --> pool-B
	// pool-A --> pool-C

	// pool-A 负责判断是奇数还是偶数，如果是偶数就分发给pool-B，如果是奇数就分发给pool-C
	// pool-B 负责把分发过来的偶数相加统计偶数和
	// pool-C 负责把分发过来的奇数相加统计奇数和

	poolA := goroutine_pool.NewGoroutinePoolWithDefaultOptions()
	poolA.Options.PoolName = "pool-A"
	poolA.Options.TaskPayloadConsumeFunc = func(ctx context.Context, pool *goroutine_pool.GoroutinePool, worker *goroutine_pool.Consumer, taskPayload any) error {
		num := taskPayload.(int)
		if num%2 == 0 {
			err := pool.SubmitNextTaskPayloadByPoolName(ctx, "pool-B", num)
			if err != nil {
				panic(err)
			}
		} else {
			err := pool.SubmitNextTaskPayloadByPoolName(ctx, "pool-C", num)
			if err != nil {
				panic(err)
			}
		}
		return nil
	}

	resultB := atomic.Int64{}
	poolB := goroutine_pool.NewGoroutinePoolWithDefaultOptions()
	poolB.Options.SetPoolName("pool-B")
	poolB.Options.TaskPayloadConsumeFunc = func(ctx context.Context, pool *goroutine_pool.GoroutinePool, worker *goroutine_pool.Consumer, taskPayload any) error {
		num := taskPayload.(int)
		resultB.Add(int64(num))
		return nil
	}
	poolA.AddNextPool(poolB)

	resultC := atomic.Int64{}
	poolC := goroutine_pool.NewGoroutinePoolWithDefaultOptions()
	poolC.Options.SetPoolName("pool-C")
	poolC.Options.TaskPayloadConsumeFunc = func(ctx context.Context, pool *goroutine_pool.GoroutinePool, worker *goroutine_pool.Consumer, taskPayload any) error {
		num := taskPayload.(int)
		resultC.Add(int64(num))
		return nil
	}
	poolA.AddNextPool(poolC)

	for i := 0; i < 10; i++ {
		_ = poolA.SubmitTaskByPayload(context.Background(), i)
	}

	poolA.ShutdownAndAwait()

	fmt.Println(resultB.Load())
	fmt.Println(resultC.Load())

}

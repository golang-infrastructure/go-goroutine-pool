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
	poolA.PoolName = "pool-A"
	poolA.TaskPayloadConsumeFunc = func(ctx context.Context, pool *goroutine_pool.GoroutinePool, worker *goroutine_pool.Consumer, taskPayload any) error {
		num := taskPayload.(int)
		if num%2 == 0 {
			_ = pool.SubmitNextTaskByPayloadToPool(ctx, num, "pool-B")
		} else {
			_ = pool.SubmitNextTaskByPayloadToPool(ctx, num, "pool-C")
		}
		return nil
	}

	resultB := atomic.Int64{}
	poolB := goroutine_pool.NewGoroutinePoolWithDefaultOptions()
	poolB.SetPoolName("pool-B")
	poolB.TaskPayloadConsumeFunc = func(ctx context.Context, pool *goroutine_pool.GoroutinePool, worker *goroutine_pool.Consumer, taskPayload any) error {
		num := taskPayload.(int)
		resultB.Add(int64(num))
		return nil
	}
	poolA.AddNextPool(poolB)

	resultC := atomic.Int64{}
	poolC := goroutine_pool.NewGoroutinePoolWithDefaultOptions()
	poolC.SetPoolName("pool-C")
	poolB.TaskPayloadConsumeFunc = func(ctx context.Context, pool *goroutine_pool.GoroutinePool, worker *goroutine_pool.Consumer, taskPayload any) error {
		num := taskPayload.(int)
		resultC.Add(int64(num))
		return nil
	}
	poolA.AddNextPool(poolC)

	for i := 0; i < 10; i++ {
		_ = poolA.SubmitTaskByPayload(context.Background(), i)
	}

	poolA.ShutdownAndAwaitDAG()

	fmt.Println(resultB.Load())
	fmt.Println(resultC.Load())

}

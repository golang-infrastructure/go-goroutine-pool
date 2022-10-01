package go_goroutine_pool

import (
	"context"
	"sync"
	"time"
)

// ------------------------------------------------ ConsumerManager ----------------------------------------------------

type ConsumerManager struct {

	// 为哪个协程池工作
	pool *GoroutinePool

	// 用于协调消费者
	consumerWg sync.WaitGroup

	// 当前在接受管理的消费者都有哪些
	consumersLock sync.RWMutex
	consumers     map[*Consumer]struct{}

	ctx        context.Context
	cancelFunc context.CancelFunc
}

func NewConsumerManager(pool *GoroutinePool) *ConsumerManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &ConsumerManager{
		pool:       pool,
		consumers:  make(map[*Consumer]struct{}),
		ctx:        ctx,
		cancelFunc: cancel,
	}
}

// Run 管理进程
func (x *ConsumerManager) Run() {

	// 初始化启动一次，先保证一部分Consumer启动起来了
	x.check()

	// 然后再启动定时检查的任务
	go func() {

		ticker := time.NewTicker(x.pool.options.ConsumerIdleCheckInterval)
		defer ticker.Stop()
		for {

			// 把这个放前面，可以快速展开Consumer的数量到MAX，如果任务比较多的话
			x.check()

			select {
			case <-ticker.C:
			case <-x.ctx.Done():
				return
			}
		}

	}()
}

func (x *ConsumerManager) check() {
	idleConsumers := x.findIdleConsumers()

	// 如果当前消费者数量小于要求的，则增加到要求的最小的消费者数量
	if uint64(len(x.consumers)) < x.pool.options.MinConsumerNum {
		for i := uint64(0); i < x.pool.options.MinConsumerNum; i++ {
			if !x.startConsumer(x.ctx) {
				return
			}
		}
		// 增加到数量之后就不再理睬，如果需要更多的消费者的话，就依赖下一次检查再继续增加
		return
	}

	// 如果有很多任务都没有处理，并且当前的消费者都处于繁忙状态，说明负载比较高，则将消费者逐步增加
	// TODO 增加的时候设计一个增加算法
	for x.pool.TaskQueueSize() != 0 && uint64(len(x.consumers)) < x.pool.options.MaxConsumerNum {
		// 启动新的任务，直到把队列处理完或者消费者数量达到最大限制
		if !x.startConsumer(x.ctx) {
			return
		}
	}

	// 任务队列为空了，并且当前有很多消费者处于空闲状态，则将多余的消费者都释放掉，保持一个最小值就可以了
	if x.pool.TaskQueueSize() == 0 && uint64(len(x.consumers)) >= x.pool.options.MinConsumerNum && len(idleConsumers) != 0 {
		// TODO 算的时候是不是要上锁?
		needShutdownConsumerCount := len(x.consumers) - int(x.pool.options.MinConsumerNum)
		for i := 0; i < needShutdownConsumerCount; i++ {
			consumer := idleConsumers[i]
			consumer.Shutdown()
			consumer.Await()
			delete(x.consumers, consumer)
		}
	}

}

// 寻找处于空闲状态的消费者
func (x *ConsumerManager) findIdleConsumers() []*Consumer {
	idleConsumers := make([]*Consumer, 0)
	for consumer := range x.consumers {
		idleConsumers = append(idleConsumers, consumer)
	}
	return idleConsumers
}

// Shutdown 关闭管理器，同时强制关闭所有管理器创建的消费者
func (x *ConsumerManager) Shutdown() {

	x.consumersLock.RLock()
	defer x.consumersLock.RUnlock()

	// 关闭每一个自己管理的客户端，先关闭着
	for consumer := range x.consumers {
		consumer.Shutdown()
	}
}

// Await 等待所有消费者都退出后自己也退出
func (x *ConsumerManager) Await() {

	x.consumersLock.RLock()
	defer x.consumersLock.RUnlock()

	// 等候等待它们真正的关闭完成
	for consumer := range x.consumers {
		consumer.Await()
	}
	x.cancelFunc()
}

// 启动一个消费者
// return: true表示成功启动了消费者，false表示启动失败
func (x *ConsumerManager) startConsumer(ctx context.Context) bool {

	consumer := NewConsumer(x.pool.options)

	// 如果配置了消费者初始化的回调事件，则执行一下回调事件
	if x.pool.options.ConsumerInitCallback != nil {
		// 如果初始化方法返回一个error，则认为是中断此Consumer启动的信号，则不再继续启动流程
		err := x.pool.options.ConsumerInitCallback(ctx, x.pool, consumer)
		if err != nil {
			return false
		}
	}

	// 加入到全家桶管理
	x.consumers[consumer] = struct{}{}

	// 启动Worker
	x.consumerWg.Add(1)
	go func() {
		defer x.consumerWg.Done()
		defer func() {
			if x.pool.options.ConsumerExitCallback != nil {
				ctx, cancelFunc := context.WithTimeout(context.Background(), time.Minute*5)
				defer cancelFunc()
				x.pool.options.ConsumerExitCallback(ctx, x.pool, consumer)
			}
		}()
		consumer.Consume(x.pool)
	}()

	return true
}

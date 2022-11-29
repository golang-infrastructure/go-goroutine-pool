package goroutine_pool

import (
	"sync"
	"time"
)

// ------------------------------------------------ ConsumerState ------------------------------------------------------

// ConsumerState 用于表示消费者的状态信息
type ConsumerState int

const (

	// ConsumerStateIdle 消费者处于空闲状态
	ConsumerStateIdle = iota

	// ConsumerStateBusy 消费者在忙着处理任务
	ConsumerStateBusy

	// ConsumerStateShutdown 消费者已经退出，不会再参与任务处理
	ConsumerStateShutdown
)

// ------------------------------------------------ Consumer -----------------------------------------------------------

// Consumer 用于处理协程池中的任务的一个消费者协程
type Consumer struct {

	// 当前消费者自己独享的一个存储空间，可以进行一些存储之类的，比如每个消费者初始化一个数据库连接之类的或其他资源，还可避免出现锁竞争的情况
	*ConsumerStorage

	// 当前消费者绑定到的协程池，每个消费者仅且绑定到一个线程池
	pool *GoroutinePool

	// 消费者的状态
	stateLock sync.RWMutex
	state     ConsumerState

	// 当前消费者上次消费到任务的时间，用于统计消费者的空闲时间
	lastConsumerTimeLock sync.RWMutex
	lastConsumeTime      time.Time

	// 表示此消费者是否是最终退出完成，消费者状态修改为ConsumerStateShutdown的时候手头可能还会有任务没处理完
	// 那么它不会接受新的任务，但是会把手头的这个任务处理完，这个标志位就是用来区分是否是真的结束了
	consumeWg sync.WaitGroup
}

// NewConsumer 创建一个新的消费者
func NewConsumer(pool *GoroutinePool) *Consumer {
	return &Consumer{
		ConsumerStorage: NewWorkerStorage(),

		pool: pool,

		// 刚创建完是空闲状态
		state:     ConsumerStateIdle,
		stateLock: sync.RWMutex{},

		lastConsumeTime:      time.Now(),
		lastConsumerTimeLock: sync.RWMutex{},

		consumeWg: sync.WaitGroup{},
	}
}

// State 获取消费者的状态
func (x *Consumer) State() ConsumerState {
	x.stateLock.RLock()
	defer x.stateLock.RUnlock()
	return x.state
}

// changeState 改变消费者的状态，会将状态修改为给定的状态，并返回上次的状态
func (x *Consumer) changeState(state ConsumerState) ConsumerState {
	x.stateLock.Lock()
	defer x.stateLock.Unlock()
	lastState := x.state
	x.state = state
	return lastState
}

// 仅当Consumer的状态和期望的一致时才更新它的状态为新的状态，否则不更新
// return: 是否更新成功，true表示更新成功，false表示未更新
func (x *Consumer) compareAndChangeState(except ConsumerState, newState ConsumerState) bool {
	x.stateLock.Lock()
	defer x.stateLock.Unlock()
	if x.state != except {
		return false
	}
	x.state = newState
	return true
}

// Idle 当前消费者距离上次有任务已经过去多长时间了
func (x *Consumer) Idle() time.Duration {
	x.lastConsumerTimeLock.RLock()
	defer x.lastConsumerTimeLock.RUnlock()
	return time.Now().Sub(x.lastConsumeTime)
}

// IsIdle 当前消费者是否处于闲置状态
func (x *Consumer) IsIdle() bool {
	return x.Idle() >= x.pool.options.ConsumerMaxIdle
}

// 更新最后一次消费任务的时间
func (x *Consumer) updateLastConsumerTime() {
	x.lastConsumerTimeLock.RLock()
	defer x.lastConsumerTimeLock.RUnlock()
	x.lastConsumeTime = time.Now()
}

// Consume 阻塞性的任务，会一直消费给定的池子中的任务直到自己被关闭或者任务队列消费完毕
func (x *Consumer) Consume(pool *GoroutinePool) {

	// 用于判断Consume是否退出
	x.consumeWg.Add(1)
	defer x.consumeWg.Done()

	x.pool = pool

	for {

		// 如果消费者已经被关闭，则不再尝试消费
		if x.State() == ConsumerStateShutdown {
			return
		}

		select {
		case task, ok := <-pool.taskChannel:
			if !ok {
				x.Shutdown()
				return
			}
			x.runTask(task)
			continue
		case <-time.After(time.Second * 3):
		}
	}
}

func (x *Consumer) Await() {
	x.consumeWg.Wait()
}

// 真正执行任务的方法，一旦进入了此方法，无论Consumer是什么状态，都会保证此任务得到执行
func (x *Consumer) runTask(task *Task) {

	// 防止退出状态被覆盖，这样子即使是刚刚进去到此方法之后状态被改变
	// 也会在执行完当前任务之后下一次循环时检测到退出状态从而退出
	lastState := x.changeState(ConsumerStateBusy)

	// 如果状态不是Busy了，说明在执行任务的时候又有人修改状态了，则保持那个人的状态即可，否则的话一律认为是应该恢复到空闲状态
	defer x.compareAndChangeState(ConsumerStateBusy, lastState)

	// 任务开始的时候更新一次最后执行任务的时间
	x.updateLastConsumerTime()

	// 任务执行完的时候也更新最后一次消费任务的时间
	// 把消费时间看做是一个时间段，在段的两头都更新一次
	defer x.updateLastConsumerTime()

	// 如果开启了panic捕捉，则启动一个panic检测
	if x.pool.options.isRunTaskEnablePanicRecovery() {
		defer func() {
			// 如果发生了panic，并且设置响应的处理方法的话，则调用其来处理panic
			if r := recover(); r != nil && x.pool.options.RunTaskEnablePanicRecoveryFunc != nil {
				x.pool.options.RunTaskEnablePanicRecoveryFunc(nil, x.pool, x, task, r)
			}
		}()
	}

	// 执行任务，根据不同的任务类型有不同的执行方式
	ctx, cancelFunc := x.pool.options.getTaskRunContextLimit()
	defer cancelFunc()

	switch task.TaskType {
	case TaskTypeFunc:
		err := task.TaskFunc(ctx, x.pool, x)
		if err != nil && x.pool.options.TaskErrorCallback != nil {
			x.pool.options.TaskErrorCallback(ctx, x.pool, x, task, err)
		}
	case TaskTypePayload:
		// payload类型的任务必须有一个执行函数，要不然没办法执行啊
		if x.pool.options.PayloadConsumeFunc == nil {
			panic("payload type task must have set CreateGoroutinePoolOptions.PayloadConsumeFunc field")
		}
		err := x.pool.options.PayloadConsumeFunc(ctx, x.pool, x, task.TaskPayload)
		if err != nil && x.pool.options.TaskErrorCallback != nil {
			x.pool.options.TaskErrorCallback(ctx, x.pool, x, task, err)
		}
	}

}

// Shutdown 让此消费者退出，如果退出的时候有任务正在执行，则会执行完手头的任务再退出
func (x *Consumer) Shutdown() {
	// 所谓的退出也只是改变一下消费者的状态，让它自己检测到状态改变自己退出
	x.changeState(ConsumerStateShutdown)
}

// IsShutdown 当前消费者是否处于关闭状态
func (x *Consumer) IsShutdown() bool {
	return x.State() == ConsumerStateShutdown
}

// ------------------------------------------------ ConsumerStorage ----------------------------------------------------

// ConsumerStorage 保证每个worker都有自己单独的Storage空间用来暂存一些东西
// Note: 非线程安全，必须保证所有操作都在同一个协程中
type ConsumerStorage struct {
	storageMap map[string]any
}

func NewWorkerStorage() *ConsumerStorage {
	return &ConsumerStorage{
		storageMap: make(map[string]any),
	}
}

// Store 把内容暂存到Consumer的存储空间中
func (x *ConsumerStorage) Store(key string, value any) {
	x.storageMap[key] = value
}

// Load 从Consumer的存储空间读取内容
func (x *ConsumerStorage) Load(key string) any {
	return x.storageMap
}

// ---------------------------------------------------------------------------------------------------------------------

package goroutine_pool

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

// ---------------------------------------------------------------------------------------------------------------------

// TaskFunc 函数类型的任务，提交一个符合此签名的任务到队列中作为一个任务，被调度的时候会执行此任务
// 如果此任务有参数需要传递，通过闭包来传递，暂不支持通过形参传递
// ctx: Context
// pool: 执行任务的池子
// worker: 执行任务的consumer
type TaskFunc func(ctx context.Context, pool *GoroutinePool, worker *Consumer) error

// TaskPayloadConsumeFunc payload类型的任务，需要一个公共的能够执行payload的函数，这个函数需要符合此签名
// ctx: Context
// pool: 执行任务的池子
// worker: 执行任务的consumer
// taskPayload: 被执行的任务的payload，任务的每个实例都会不同，比如可能是爬虫任务的url，可能是etl任务的每条数据的id
type TaskPayloadConsumeFunc func(ctx context.Context, pool *GoroutinePool, worker *Consumer, taskPayload any) error

// ------------------------------------------------ TaskType -----------------------------------------------------------

// TaskType 任务类型，支持函数类型和payload类型
type TaskType int

const (

	// TaskTypeNone 未指定类型时的初始值
	TaskTypeNone TaskType = iota

	// TaskTypeFunc 函数类型的任务就是向队列中提交一个函数，任务的逻辑是封装在这个函数中的，执行任务的时候会直接执行这个函数
	TaskTypeFunc

	// TaskTypePayload 值类型的任务，封装的是任务的一些参数，需要依赖ConsumerPayloadFunc来执行
	TaskTypePayload
)

// ------------------------------------------------ Task ---------------------------------------------------------------

// Task 表示一个待运行的任务
type Task struct {

	// 任务的类型
	TaskType TaskType

	// 任务是一个直接运行的函数，那就直接运行
	TaskFunc TaskFunc

	// 任务是提交的一些值参数，传递给额外配置的worker来运行
	TaskPayload any
}

func NewTaskByFunc(taskFunc TaskFunc) *Task {
	return &Task{
		TaskType: TaskTypeFunc,
		TaskFunc: taskFunc,
	}
}

func NewTaskByPayload(taskPayload any) *Task {
	return &Task{
		TaskType:    TaskTypePayload,
		TaskPayload: taskPayload,
	}
}

// ------------------------------------------------ GoroutinePool ------------------------------------------------------

// GoroutinePool 协程池
// 泛型参数是任务的payload的类型
type GoroutinePool struct {

	// 协程池的运行参数
	Options *CreateGoroutinePoolOptions

	// 任务队列，有缓冲大小的channel，大小由 Options.PoolTaskQueueMaxLength 决定
	taskChannel chan *Task

	// 池子中的任务需要有消费者来消费，池子并不直接管理消费者，消费者管理器就是用来管理消费者的
	consumerManager *ConsumerManager

	// 用于标识任务队列是否被关闭，当任务队列被关闭就不会再接收新的任务提交了
	taskChannelShutdown atomic.Bool

	// 连接的下一个协程池，通常在构建DAG的时候有用，属于高级用法
	// 一个协程池可以由多个后记协程池，在当前协程池中执行的任务可以向后续协程池中发送任务
	nextPoolMapLock sync.RWMutex
	nextPoolMap     map[string]*GoroutinePool
}

// NewGoroutinePoolWithDefaultOptions 创建协程池，使用默认的选项
func NewGoroutinePoolWithDefaultOptions() *GoroutinePool {
	x, _ := NewGoroutinePool(NewCreateGoroutinePoolOptions())
	return x
}

// NewGoroutinePool 创建一个协程池
func NewGoroutinePool(options *CreateGoroutinePoolOptions) (*GoroutinePool, error) {

	if options.PoolTaskQueueMaxLength == 0 {
		return nil, errors.New("Options.PoolTaskQueueMaxLength can not be 0")
	}

	pool := &GoroutinePool{
		Options:             options,
		taskChannel:         make(chan *Task, options.PoolTaskQueueMaxLength),
		taskChannelShutdown: atomic.Bool{},

		nextPoolMapLock: sync.RWMutex{},
		nextPoolMap:     make(map[string]*GoroutinePool),
	}

	// 池子中的消费者管理
	pool.consumerManager = NewConsumerManager(pool)
	pool.consumerManager.Run()

	return pool, nil
}

// ------------------------------------------------ Task ---------------------------------------------------------------

// SubmitTaskByFunc 提交一个函数作为任务运行
func (x *GoroutinePool) SubmitTaskByFunc(ctx context.Context, taskFunc TaskFunc) error {
	return x.SubmitTask(ctx, NewTaskByFunc(taskFunc))
}

// SubmitTaskByPayload 提交一个payload类型的任务
func (x *GoroutinePool) SubmitTaskByPayload(ctx context.Context, taskPayload any) error {

	// 提交payload类型的任务的时候会进行检查，如果没有设置相应的处理函数的话直接就拒绝提交任务
	if x.Options.TaskPayloadConsumeFunc == nil {
		return ErrPayloadConsumeFuncNil
	}

	return x.SubmitTask(ctx, NewTaskByPayload(taskPayload))
}

// SubmitTask 提交一个任务到任务队列
func (x *GoroutinePool) SubmitTask(ctx context.Context, task *Task) error {
	select {
	case x.taskChannel <- task:
		return nil
	case <-ctx.Done():
		return context.DeadlineExceeded
	}
}

// TaskQueueSize 任务队列中还剩下多少个任务没有执行
func (x *GoroutinePool) TaskQueueSize() int {
	return len(x.taskChannel)
}

// ------------------------------------------------ next -------------------------------------------------------------

// AddNextPool 协程池允许桥接，桥接的协程池可以把任务发送到下一个池子中，这样当有很多个互相依赖的任务的时候，就可以自己用线程池搭建DAG
func (x *GoroutinePool) AddNextPool(pool *GoroutinePool) *GoroutinePool {

	x.nextPoolMapLock.Lock()
	defer x.nextPoolMapLock.Unlock()

	// 标记next
	x.nextPoolMap[pool.Options.PoolName] = pool

	return x
}

// RemoveNextPool 移除next pool
func (x *GoroutinePool) RemoveNextPool(pool *GoroutinePool) *GoroutinePool {
	x.nextPoolMapLock.Lock()
	defer x.nextPoolMapLock.Unlock()

	// 删除next标记
	delete(x.nextPoolMap, pool.Options.PoolName)

	return x
}

// ------------------------------------------------- --------------------------------------------------------------------

func (x *GoroutinePool) SubmitNextTaskPayload(ctx context.Context, taskPayload any) error {
	return x.SubmitNextTaskPayloadByFunc(ctx, nil, taskPayload)
}

// SubmitNextTaskPayloadByPoolName 根据协程池的名称提交Payload类型的任务，当DAG有多个后续协程池时使用
func (x *GoroutinePool) SubmitNextTaskPayloadByPoolName(ctx context.Context, poolName string, taskPayload any) error {
	x.nextPoolMapLock.Lock()
	defer x.nextPoolMapLock.Unlock()

	nextPool, exists := x.nextPoolMap[poolName]
	if !exists {
		return ErrPoolNotFound
	}
	return nextPool.SubmitTaskByPayload(ctx, taskPayload)
}

// SubmitNextTaskPayloadByFunc 往下一个阶段的池子中提交任务，如果下一个阶段的池子有多个，则每个都会被提交一个任务
// ctx: 超时
// Task: 任务的payload
func (x *GoroutinePool) SubmitNextTaskPayloadByFunc(ctx context.Context, chooseNextPoolFunc func(pool *GoroutinePool) bool, taskPayload any) error {

	x.nextPoolMapLock.RLock()
	defer x.nextPoolMapLock.RUnlock()

	for _, pool := range x.nextPoolMap {
		// 路由筛选
		if chooseNextPoolFunc != nil && !chooseNextPoolFunc(pool) {
			continue
		}
		select {
		case pool.taskChannel <- NewTaskByPayload(taskPayload):
		case <-ctx.Done():
			return context.DeadlineExceeded
		}
	}
	return nil
}

func (x *GoroutinePool) SubmitNextTaskFunc(ctx context.Context, taskFunc TaskFunc) error {
	return x.SubmitNextTaskFuncByFunc(ctx, nil, taskFunc)
}

// SubmitNextTaskFuncByPoolName 根据协程池的名字提交后续的任务，当DAG有多个后续协程池时使用
func (x *GoroutinePool) SubmitNextTaskFuncByPoolName(ctx context.Context, poolName string, taskFunc TaskFunc) error {
	x.nextPoolMapLock.RLock()
	defer x.nextPoolMapLock.RUnlock()

	nextPool, exists := x.nextPoolMap[poolName]
	if !exists {
		return ErrPoolNotFound
	}
	return nextPool.SubmitTaskByFunc(ctx, taskFunc)
}

// SubmitNextTaskFuncByFunc 往下一个阶段的池子中提交函数类型的任务
func (x *GoroutinePool) SubmitNextTaskFuncByFunc(ctx context.Context, chooseNextPoolFunc func(pool *GoroutinePool) bool, taskFunc TaskFunc) error {

	x.nextPoolMapLock.RLock()
	defer x.nextPoolMapLock.RUnlock()

	for _, pool := range x.nextPoolMap {
		// 路由筛选
		if chooseNextPoolFunc != nil && !chooseNextPoolFunc(pool) {
			continue
		}
		select {
		case pool.taskChannel <- &Task{
			TaskType: TaskTypeFunc,
			TaskFunc: taskFunc,
		}:
		case <-ctx.Done():
			return context.DeadlineExceeded
		}
	}
	return nil
}

// ------------------------------------------------- --------------------------------------------------------------------

// IsShutdown 支持是否已经关闭提交任务
func (x *GoroutinePool) IsShutdown() bool {
	return x.taskChannelShutdown.Load()
}

// shutdown 关闭当前池子的任务队列，不再接收新的任务
func (x *GoroutinePool) shutdown() *GoroutinePool {
	// 先修改状态阻塞写入，注意不要调换顺序，不然可能会导致写close状态的channel
	x.taskChannelShutdown.Store(true)
	// 然后再将其关闭
	close(x.taskChannel)
	return x
}

// await 等待当前任务支持关闭
func (x *GoroutinePool) await() {
	x.consumerManager.Await()
}

// ShutdownAndAwait 关闭当前线程池，并等待当前线程池运行完毕
func (x *GoroutinePool) ShutdownAndAwait() {

	// 关闭自己的队列
	x.shutdown()

	// 等待自己的消费者退出
	x.await()

	// 自己的消费者相当于是后续pool的生产者，如果生产者退出了，则可以认为不再有新的任务产生了，则可以把后续池子的任务队列都关闭掉
	// 然后递归等待后续池子完毕，会遍历整张DAG上的池子都完毕
	nextPoolSlice := x.copyNextPoolSlice()
	for _, pool := range nextPoolSlice {
		pool.ShutdownAndAwait()
	}

}

// 在访问的时候做个快照，访问的是快照
func (x *GoroutinePool) copyNextPoolSlice() map[string]*GoroutinePool {

	x.nextPoolMapLock.RLock()
	defer x.nextPoolMapLock.RUnlock()

	// copy
	nextPoolMapCopy := make(map[string]*GoroutinePool, len(x.nextPoolMap))
	for name, nextPool := range x.nextPoolMap {
		nextPoolMapCopy[name] = nextPool
	}
	return nextPoolMapCopy
}

// ---------------------------------------------------------------------------------------------------------------------

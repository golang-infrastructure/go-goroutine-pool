package goroutine_pool

import (
	"context"
	"sync"
	"sync/atomic"
)

// ---------------------------------------------------------------------------------------------------------------------

// TaskFunc 函数类型的任务，提交一个符合此签名的任务到队列中作为一个任务，被调度的时候会执行此任务
// 如果此任务有参数需要传递，通过闭包来传递，暂不支持通过形参传递
type TaskFunc func(ctx context.Context, pool *GoroutinePool, worker *Consumer) error

// TaskPayloadConsumeFunc payload类型的任务，需要一个公共的能够执行payload的函数，这个函数需要符合此签名
type TaskPayloadConsumeFunc func(ctx context.Context, pool *GoroutinePool, worker *Consumer, taskPayload any) error

// ------------------------------------------------ TaskType -----------------------------------------------------------

// TaskType 任务类型，支持函数类型和payload类型
type TaskType int

const (

	// TaskTypeFunc 函数类型的任务就是向队列中提交一个函数，任务的逻辑是封装在这个函数中的
	TaskTypeFunc TaskType = iota

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

// ------------------------------------------------ GoroutinePool ------------------------------------------------------

// GoroutinePool 协程池
// 泛型参数是任务的payload的类型
type GoroutinePool struct {

	// 协程池的运行参数
	*CreateGoroutinePoolOptions

	// 任务队列，有缓冲大小的channel，大小由options.PoolTaskQueueMaxLength决定
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
	return NewGoroutinePool(NewCreateGoroutinePoolOptions())
}

// NewGoroutinePool 创建一个协程池
func NewGoroutinePool(options *CreateGoroutinePoolOptions) *GoroutinePool {

	pool := &GoroutinePool{
		CreateGoroutinePoolOptions: options,
		// TODO 从配置中获取值
		taskChannel:         make(chan *Task, options.PoolTaskQueueMaxLength),
		taskChannelShutdown: atomic.Bool{},

		nextPoolMapLock: sync.RWMutex{},
		nextPoolMap:     make(map[string]*GoroutinePool),
	}

	// 池子中的消费者管理
	pool.consumerManager = NewConsumerManager(pool)
	pool.consumerManager.Run()

	return pool
}

// ------------------------------------------------ Task ---------------------------------------------------------------

// SubmitTaskByFunc 提交一个函数作为任务运行
func (x *GoroutinePool) SubmitTaskByFunc(ctx context.Context, taskFunc TaskFunc) error {
	task := &Task{
		TaskType: TaskTypeFunc,
		TaskFunc: taskFunc,
	}
	select {
	case x.taskChannel <- task:
		return nil
	case <-ctx.Done():
		return context.DeadlineExceeded
	}
}

// SubmitTaskByPayload 提交一个payload类型的任务
func (x *GoroutinePool) SubmitTaskByPayload(ctx context.Context, taskPayload any) error {

	// 提交payload类型的任务的时候会进行检查，如果没有设置相应的处理函数的话直接就拒绝提交任务
	if x.TaskPayloadConsumeFunc == nil {
		return ErrPayloadConsumeFuncNil
	}

	task := &Task{
		TaskType:    TaskTypePayload,
		TaskPayload: taskPayload,
	}
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
	defer x.nextPoolMapLock.Lock()

	x.nextPoolMap[pool.PoolName] = pool

	return x
}

// SubmitNextTaskByPayloadToPool 根据协程池的名称提交Payload类型的任务，当DAG有多个后续协程池时使用
func (x *GoroutinePool) SubmitNextTaskByPayloadToPool(ctx context.Context, taskPayload any, poolName string) error {
	nextPool, exists := x.nextPoolMap[poolName]
	if !exists {
		return ErrPoolNotFound
	}
	return nextPool.SubmitTaskByPayload(ctx, taskPayload)
}

// SubmitNextTaskByPayload 往下一个阶段的池子中提交任务，如果下一个阶段的池子有多个，则每个都会被提交一个任务
// ctx: 超时
// Task: 任务的payload
func (x *GoroutinePool) SubmitNextTaskByPayload(ctx context.Context, taskPayload any, chooseNextPoolFunc ...func(pool *GoroutinePool) bool) error {

	x.nextPoolMapLock.RLock()
	defer x.nextPoolMapLock.RUnlock()

	for _, pool := range x.nextPoolMap {
		// 路由筛选
		if len(chooseNextPoolFunc) != 0 && !chooseNextPoolFunc[0](pool) {
			continue
		}
		select {
		case pool.taskChannel <- &Task{
			TaskType:    TaskTypePayload,
			TaskPayload: taskPayload,
		}:
		case <-ctx.Done():
			return context.DeadlineExceeded
		}
	}
	return nil
}

// SubmitNextTaskByFuncToPool 根据协程池的名字提交后续的任务，当DAG有多个后续协程池时使用
func (x *GoroutinePool) SubmitNextTaskByFuncToPool(ctx context.Context, taskFunc TaskFunc, poolName string) error {
	nextPool, exists := x.nextPoolMap[poolName]
	if !exists {
		return ErrPoolNotFound
	}
	return nextPool.SubmitTaskByFunc(ctx, taskFunc)
}

// SubmitNextTaskByFunc 往下一个阶段的池子中提交函数类型的任务
func (x *GoroutinePool) SubmitNextTaskByFunc(ctx context.Context, taskFunc TaskFunc, chooseNextPoolFunc ...func(pool *GoroutinePool) bool) error {

	x.nextPoolMapLock.RLock()
	defer x.nextPoolMapLock.RUnlock()

	for _, pool := range x.nextPoolMap {
		// 路由筛选
		if len(chooseNextPoolFunc) != 0 && !chooseNextPoolFunc[0](pool) {
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

// IsShutdown 支持是否已经关闭提交任务
func (x *GoroutinePool) IsShutdown() bool {
	return x.taskChannelShutdown.Load()
}

// Shutdown 关闭当前池子的任务队列，不再接收新的任务
func (x *GoroutinePool) Shutdown() *GoroutinePool {
	// 先修改状态阻塞写入，注意不要调换顺序，不然可能会导致写close状态的channel
	x.taskChannelShutdown.Store(true)
	// 然后再将其关闭
	close(x.taskChannel)
	return x
}

// Await 等待当前任务支持关闭
func (x *GoroutinePool) Await() {
	x.consumerManager.Await()
}

// AwaitDAG 等待整个DAG执行完毕才退出，适用于多个池子组合搭建DAG任务的场景
func (x *GoroutinePool) AwaitDAG() {
	// 先等待自己完毕
	x.Await()
	// 然后递归等待后续池子完毕，会遍历整张DAG上的池子都完毕
	for _, pool := range x.copyNextPoolSlice() {
		pool.AwaitDAG()
	}
}

// ShutdownAndAwait 关闭当前线程池，并等待当前线程池运行完毕
func (x *GoroutinePool) ShutdownAndAwait() {
	x.Shutdown()
	x.Await()
}

// TODO 2022-12-1 00:34:36 池子关闭逻辑有问题，需要再想一想应该怎么协调
// ShutdownAndAwaitDAG 关闭当前线程池，并等待整个DAG运行完毕
func (x *GoroutinePool) ShutdownAndAwaitDAG() {
	x.Shutdown()
	x.AwaitDAG()
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

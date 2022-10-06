package go_goroutine_pool

import (
	"context"
	"sync"
	"sync/atomic"
)

// ---------------------------------------------------------------------------------------------------------------------

// TaskFunc 函数类型的任务
type TaskFunc func(ctx context.Context, pool *GoroutinePool, worker *Consumer) error

// PayloadConsumeFunc payload类型的任务，需要一个公共的能够执行payload的函数
type PayloadConsumeFunc func(ctx context.Context, pool *GoroutinePool, worker *Consumer, taskPayload any) error

// TaskType 任务类型，支持函数类型和payload类型
type TaskType int

const (

	// TaskTypeFunc 函数类型的任务就是向队列中提交一个函数，任务的逻辑是封装在这个函数中的
	TaskTypeFunc TaskType = iota

	// TaskTypePayload 值类型的任务，封装的是任务的一些参数，需要依赖ConsumerPayloadFunc来执行
	TaskTypePayload
)

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

type GoroutinePool struct {

	// 协程池的运行参数
	options *CreateGoroutinePoolOptions

	// 任务队列
	taskChannel chan *Task

	// 池子中的任务需要有消费者来消费，消费者管理器就是用来管理消费者的
	consumerManager *ConsumerManager

	// 用于标识任务队列是否被关闭
	taskChannelShutdown atomic.Bool

	// 连接的下一个任务池，通常在构建DAG的时候有用
	nextPoolSliceLock sync.RWMutex
	nextPoolSlice     []*GoroutinePool
}

func NewGoroutinePool(options *CreateGoroutinePoolOptions) *GoroutinePool {

	pool := &GoroutinePool{
		options: options,
		// TODO 从配置中获取值
		taskChannel:         make(chan *Task, options.PoolTaskQueueMaxLength),
		taskChannelShutdown: atomic.Bool{},
	}

	// 池子中的消费者管理
	pool.consumerManager = NewConsumerManager(pool)
	pool.consumerManager.Run()

	return pool
}

// ------------------------------------------------ Task ---------------------------------------------------------------

// SubmitTaskByFunc 提交一个函数作为任务运行
func (x *GoroutinePool) SubmitTaskByFunc(ctx context.Context, taskFunc TaskFunc) error {
	select {
	case x.taskChannel <- &Task{
		TaskType: TaskTypeFunc,
		TaskFunc: taskFunc,
	}:
		return nil
	case <-ctx.Done():
		return ErrContextTimeout
	}
}

// SubmitTaskByPayload 提交一个payload类型的任务
func (x *GoroutinePool) SubmitTaskByPayload(ctx context.Context, taskPayload any) error {

	// 提交payload类型的任务的时候会进行检查，如果没有设置相应的处理函数的话直接就拒绝提交任务
	if x.options.PayloadConsumeFunc == nil {
		return ErrPayloadConsumeFuncNil
	}

	select {
	case x.taskChannel <- &Task{
		TaskType:    TaskTypePayload,
		TaskPayload: taskPayload,
	}:
		return nil
	case <-ctx.Done():
		return ErrContextTimeout
	}
}

// TaskQueueSize 任务队列中还剩下多少个任务没有执行
func (x *GoroutinePool) TaskQueueSize() int {
	return len(x.taskChannel)
}

// ------------------------------------------------ next -------------------------------------------------------------

// AddNextPool 协程池允许桥接，桥接的协程池可以把任务发送到下一个池子中，这样当有很多个互相依赖的任务的时候，就可以自己用线程池搭建DAG
func (x *GoroutinePool) AddNextPool(pool *GoroutinePool) *GoroutinePool {

	x.nextPoolSliceLock.Lock()
	defer x.nextPoolSliceLock.Lock()

	x.nextPoolSlice = append(x.nextPoolSlice, pool)

	return x
}

// SubmitNextTaskByPayload 往下一个阶段的池子中提交任务，如果下一个阶段的池子有多个，则每个都会被提交一个任务
// ctx: 超时
// Task: 任务的payload
func (x *GoroutinePool) SubmitNextTaskByPayload(ctx context.Context, taskPayload any) error {

	x.nextPoolSliceLock.RLock()
	defer x.nextPoolSliceLock.RUnlock()

	for _, pool := range x.nextPoolSlice {
		select {
		case pool.taskChannel <- &Task{
			TaskType:    TaskTypePayload,
			TaskPayload: taskPayload,
		}:
		case <-ctx.Done():
			return ErrContextTimeout
		}
	}
	return nil
}

// SubmitNextTaskByFunc 往下一个阶段的池子中提交函数类型的任务
func (x *GoroutinePool) SubmitNextTaskByFunc(ctx context.Context, taskFunc TaskFunc) error {

	x.nextPoolSliceLock.RLock()
	defer x.nextPoolSliceLock.RUnlock()

	for _, pool := range x.nextPoolSlice {
		select {
		case pool.taskChannel <- &Task{
			TaskType: TaskTypeFunc,
			TaskFunc: taskFunc,
		}:
		case <-ctx.Done():
			return ErrContextTimeout
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

func (x *GoroutinePool) ShutdownAndAwait() {
	x.Shutdown()
	x.Await()
}

func (x *GoroutinePool) ShutdownAndAwaitDAG() {
	x.Shutdown()
	x.AwaitDAG()
}

func (x *GoroutinePool) copyNextPoolSlice() []*GoroutinePool {

	x.nextPoolSliceLock.RLock()
	defer x.nextPoolSliceLock.RUnlock()

	// copy
	nextPoolSliceCopy := make([]*GoroutinePool, len(x.nextPoolSlice))
	for index, nextPool := range x.nextPoolSlice {
		nextPoolSliceCopy[index] = nextPool
	}
	return nextPoolSliceCopy
}

// ---------------------------------------------------------------------------------------------------------------------

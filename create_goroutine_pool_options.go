package go_goroutine_pool

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"
)

const (
	// DefaultTaskQueueMaxLength 默认的任务队列
	DefaultTaskQueueMaxLength = 100_000
)

// 空闲任务的参数控制
const (
	DefaultInitConsumerNum = 100
	DefaultMaxConsumerNum  = 100
	DefaultMinConsumerNum  = 100
	DefaultConsumerMaxIdle = time.Minute * 5
)

const (
	DefaultConsumerIdleCheckInterval = time.Minute
	DefaultRunTaskTimeout            = time.Minute * 5
)

// CreateGoroutinePoolOptions 创建协程池的各种选项
type CreateGoroutinePoolOptions struct {

	// 可以为pool取一个更便于理解的名字，如果没有取的话，则会默认生成一个
	PoolName string

	// 任务队列相关
	PoolTaskQueueMaxLength uint64

	// 使用payload形式提交的任务需要的运行函数
	PayloadConsumeFunc PayloadConsumeFunc

	// 最少有几个人在执行
	InitConsumerNum uint64
	// 最多工作的worker数
	MaxConsumerNum uint64
	// 最小工作的worker数
	MinConsumerNum uint64
	// 当worker空闲超出多长时间之后将其释放掉
	ConsumerMaxIdle time.Duration

	// 当执行的任务返回error的时候的回调方法
	TaskErrorCallback func(ctx context.Context, pool *GoroutinePool, consumer *Consumer, task *Task, err error)

	// 每个Worker初始化的时候调用一次，当返回的error不为空的会放弃启动此Consumer
	ConsumerInitCallback func(ctx context.Context, pool *GoroutinePool, consumer *Consumer) error

	// 每个Worker退出的时候调用一次，包括正常退出和空闲太久被退出
	ConsumerExitCallback func(ctx context.Context, pool *GoroutinePool, consumer *Consumer)

	// TODO 空闲检查每隔多长时间进行一次
	ConsumerIdleCheckInterval time.Duration

	// Worker执行任务的时候是否开启全panic捕获，防止异常退出，
	RunTaskEnablePanicRecovery *bool
	// 当执行任务的时候发生panic的时候会执行此方法
	RunTaskEnablePanicRecoveryFunc func(ctx context.Context, pool *GoroutinePool, consumer *Consumer, task *Task, recoveryResult any)
	RunTaskTimeout                 time.Duration
}

func NewCreateGoroutinePoolOptions() *CreateGoroutinePoolOptions {
	return &CreateGoroutinePoolOptions{
		PoolName:               genPoolName(),
		PoolTaskQueueMaxLength: DefaultTaskQueueMaxLength,
		PayloadConsumeFunc:     nil,

		InitConsumerNum: DefaultInitConsumerNum,
		MaxConsumerNum:  DefaultMaxConsumerNum,
		MinConsumerNum:  DefaultMinConsumerNum,
		ConsumerMaxIdle: DefaultConsumerMaxIdle,

		TaskErrorCallback:         nil,
		ConsumerInitCallback:      nil,
		ConsumerExitCallback:      nil,
		ConsumerIdleCheckInterval: DefaultConsumerIdleCheckInterval,

		RunTaskEnablePanicRecovery:     nil,
		RunTaskEnablePanicRecoveryFunc: nil,
		RunTaskTimeout:                 DefaultRunTaskTimeout,
	}
}

func (x *CreateGoroutinePoolOptions) SetPoolName(poolName string) *CreateGoroutinePoolOptions {
	x.PoolName = poolName
	return x
}

func (x *CreateGoroutinePoolOptions) SetPoolTaskQueueMaxLength(taskQueueMaxLength uint64) *CreateGoroutinePoolOptions {
	x.PoolTaskQueueMaxLength = taskQueueMaxLength
	return x
}

func (x *CreateGoroutinePoolOptions) SetPayloadConsumeFunc(payloadConsumeFunc PayloadConsumeFunc) *CreateGoroutinePoolOptions {
	x.PayloadConsumeFunc = payloadConsumeFunc
	return x
}

func (x *CreateGoroutinePoolOptions) SetInitConsumerNum(initConsumerNum uint64) *CreateGoroutinePoolOptions {
	x.InitConsumerNum = initConsumerNum
	return x
}

func (x *CreateGoroutinePoolOptions) SetMaxConsumerNum(maxConsumerNum uint64) *CreateGoroutinePoolOptions {
	x.MaxConsumerNum = maxConsumerNum
	return x
}

func (x *CreateGoroutinePoolOptions) SetMinConsumerNum(minConsumerNum uint64) *CreateGoroutinePoolOptions {
	x.MinConsumerNum = minConsumerNum
	return x
}

func (x *CreateGoroutinePoolOptions) SetConsumerMaxIdle(consumerMaxIdle time.Duration) *CreateGoroutinePoolOptions {
	x.ConsumerMaxIdle = consumerMaxIdle
	return x
}

func (x *CreateGoroutinePoolOptions) SetTaskErrorCallback(taskErrorCallback func(ctx context.Context, pool *GoroutinePool, consumer *Consumer, task *Task, err error)) *CreateGoroutinePoolOptions {
	x.TaskErrorCallback = taskErrorCallback
	return x
}

func (x *CreateGoroutinePoolOptions) SetConsumerInitCallback(consumerInitCallback func(ctx context.Context, pool *GoroutinePool, consumer *Consumer) error) *CreateGoroutinePoolOptions {
	x.ConsumerInitCallback = consumerInitCallback
	return x
}

func (x *CreateGoroutinePoolOptions) SetConsumerExitCallback(consumerExitCallback func(ctx context.Context, pool *GoroutinePool, consumer *Consumer)) *CreateGoroutinePoolOptions {
	x.ConsumerExitCallback = consumerExitCallback
	return x
}

func (x *CreateGoroutinePoolOptions) SetRunTaskEnablePanicRecovery(runTaskEnablePanicRecovery *bool) *CreateGoroutinePoolOptions {
	x.RunTaskEnablePanicRecovery = runTaskEnablePanicRecovery
	return x
}

func (x *CreateGoroutinePoolOptions) SetRunTaskEnablePanicRecoveryFunc(runTaskEnablePanicRecoveryFunc func(ctx context.Context, pool *GoroutinePool, consumer *Consumer, task *Task, recoveryResult any)) *CreateGoroutinePoolOptions {
	x.RunTaskEnablePanicRecoveryFunc = runTaskEnablePanicRecoveryFunc
	return x
}

func (x *CreateGoroutinePoolOptions) SetRunTaskTimeout(runTaskTimeout time.Duration) *CreateGoroutinePoolOptions {
	x.RunTaskTimeout = runTaskTimeout
	return x
}

// 获取任务的执行时间限制
func (x *CreateGoroutinePoolOptions) getTaskRunContextLimit() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), x.RunTaskTimeout)
}

// 是否开启执行任务时的异常捕获，一般是为了保证不被某几个任务导致整个程序crash
func (x *CreateGoroutinePoolOptions) isRunTaskEnablePanicRecovery() bool {
	return (x.RunTaskEnablePanicRecovery != nil && *x.RunTaskEnablePanicRecovery) || x.RunTaskEnablePanicRecoveryFunc != nil
}

// ---------------------------------------------------------------------------------------------------------------------

var poolIdGenerator atomic.Int64

// 用于生成一个全局唯一的名字
func genPoolName() string {
	id := poolIdGenerator.Add(1)
	return fmt.Sprintf("pool-%d", id)
}

// ---------------------------------------------------------------------------------------------------------------------

package goroutine_pool

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"
)

const (

	// TaskQueueMaxLengthUnlimited 表示队列中等待执行的任务的数量时没有限制的，会随着任务的提交自动增长
	TaskQueueMaxLengthUnlimited = 0

	// DefaultTaskQueueMaxLength 默认的任务队列的最大长度，当任务队列中挤压数超过此数量时提交任务时就会卡住直到任务队列有空闲或者超时
	DefaultTaskQueueMaxLength = TaskQueueMaxLengthUnlimited
)

// 空闲任务的参数控制
const (

	// DefaultInitConsumerNum 初始化的任务数量
	DefaultInitConsumerNum = 100

	// DefaultMaxConsumerNum 最大消费者的数量
	DefaultMaxConsumerNum = 100

	// DefaultMinConsumerNum 最小的消费者数量
	DefaultMinConsumerNum = 100

	// DefaultConsumerMaxIdle 任务空闲多久之后会被释放掉
	DefaultConsumerMaxIdle = time.Minute * 5
)

const (

	// DefaultConsumerIdleCheckInterval 默认情况下隔多久检查一下消费者是否处于空闲状态
	DefaultConsumerIdleCheckInterval = time.Minute

	// DefaultRunTaskTimeout 默认情况下任务运行多久认为是超时了
	DefaultRunTaskTimeout = time.Minute * 5
)

// CreateGoroutinePoolOptions 创建协程池的各种选项，用于深度定制协程池
type CreateGoroutinePoolOptions struct {

	// 可以为pool取一个更便于理解的名字，如果没有取的话，则会默认生成一个，这个名字在开启监控的时候可以用来区分不同的协程池
	PoolName string

	// 任务队列的最大长度，当任务队列中挤压的任务数量超过这个数字时，新提交的任务就会卡住，直到任务队列有名额或者提交超时
	PoolTaskQueueMaxLength uint64

	// 使用payload形式提交的任务需要的运行函数，这样提交任务的时候就只需要提交任务参数就可以了
	PayloadConsumeFunc TaskPayloadConsumeFunc

	// 下面这几个参数是当协程池中的协程消费者的数量需要动态的调整的时候，用来控制如何调整的
	// 初始化的时候有几个消费者在执行，协程池创建的时候就会启动这么多的消费者
	InitConsumerNum uint64
	// 最大的消费者数量，即使任务挤压再多，启动的消费者的数量也不会超过这个数量限制
	MaxConsumerNum uint64
	// 最小工作的消费者数量，
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

func (x *CreateGoroutinePoolOptions) SetPayloadConsumeFunc(payloadConsumeFunc TaskPayloadConsumeFunc) *CreateGoroutinePoolOptions {
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

// 为协程池生成一个当前进程唯一的ID
var poolIdGenerator atomic.Int64

// 用于生成一个全局唯一的名字
func genPoolName() string {
	id := poolIdGenerator.Add(1)
	return fmt.Sprintf("goroutine-pool-%d", id)
}

// ---------------------------------------------------------------------------------------------------------------------

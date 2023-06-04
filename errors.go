package goroutine_pool

import "errors"

var (
	// ErrPayloadConsumeFuncNil 项执行payload类型的任务，但是没有设置PayloadConsumeFunc，只能返回错误了
	ErrPayloadConsumeFuncNil = errors.New("Options.TaskPayloadConsumeFunc nil")

	// ErrPoolNotFound 协程池不存在
	ErrPoolNotFound = errors.New("pool not found")
)

package go_goroutine_pool

import "errors"

var (
	// ErrContextTimeout 超时
	ErrContextTimeout = errors.New("context timeout")

	// ErrPayloadConsumeFuncNil 项执行payload类型的任务，但是没有设置PayloadConsumeFunc
	ErrPayloadConsumeFuncNil = errors.New("CreateGoroutinePoolOptions.PayloadConsumeFunc nil")
)

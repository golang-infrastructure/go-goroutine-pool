package go_goroutine_pool

import "github.com/prometheus/client_golang/prometheus"

// TODO 2022-9-26 01:49:11 

// PoolPrometheus 用于对每个池子做指标统计，方便接入到可视化界面中
type PoolPrometheus struct {
	pool *GoroutinePool
}

func NewPoolPrometheus(pool *GoroutinePool) *PoolPrometheus {
	return &PoolPrometheus{
		pool: pool,
	}
}

func (x *PoolPrometheus) Add() {
	
	//
	// 当前有多少个消费者在运行
	c := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "",
	})

	c.Inc()

	// 有多少处于空闲状态，有多少处于繁忙状态，负载大概是多少
	// 过去一分钟消费了多少个任务
	// 过去五分钟消费了多少个任务
	// 过去一个小时消费了多少个任务
	// 现在还有多少个任务等待消费，一共消费了多少个任务，其中多少个成功，多少个失败，成功率是多少
	// 对于成功的任务平均每个耗时多少，对于失败的任务平均每个耗时多少
	prometheus.NewCounter(prometheus.CounterOpts{
		Name: "",
	})
}

// Register 把自己注册
func (x *PoolPrometheus) Register() {

}

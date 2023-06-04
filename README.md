# Golang协程池（Goroutine Pool）

无需创建线程池，全局限制最大运行的协程数（根据内存自动调整）

## 空闲释放

## 任务模式

两种任务模式：

- 提交任务payload和worker，使用相同的worker运行不同的payload
- 忽略payload的概念，直接运行worker函数

## 兼容复杂任务场景，多个Pool自由搭建DAG

## 一个收缩的例子

# TODO

- 支持往当前池子增加任务 





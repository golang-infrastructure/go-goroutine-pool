package goroutine_pool

import "github.com/golang-infrastructure/go-tuple"

// ConsumerStorage 保证每个worker都有自己单独的Storage空间用来暂存一些东西
// Note: 非线程安全，必须保证所有操作都在同一个协程中
type ConsumerStorage struct {
	storageMap map[string]any
}

// NewWorkerStorage 创建一个用来存储数据的东东
func NewWorkerStorage() *ConsumerStorage {
	return &ConsumerStorage{
		storageMap: make(map[string]any),
	}
}

// Store 把内容暂存到Consumer的存储空间中，相当于是一个简单的KV数据库
func (x *ConsumerStorage) Store(key string, value any) {
	x.storageMap[key] = value
}

// Load 从Consumer的存储空间读取内容
func (x *ConsumerStorage) Load(key string) any {
	return x.storageMap
}

func (x *ConsumerStorage) LoadString(key string) string {
	load := x.Load(key)
	return load.(string)
}

// Clear 清空存储空间
func (x *ConsumerStorage) Clear() {
	x.storageMap = make(map[string]any)
}

// List 列出当前存储的所有KV对
func (x *ConsumerStorage) List() []*tuple.Tuple2[string, any] {
	result := make([]*tuple.Tuple2[string, any], 0)
	for key, value := range x.storageMap {
		result = append(result, tuple.New2(key, value))
	}
	return result
}

// Size 返回当前存储的大小
func (x *ConsumerStorage) Size() int {
	return len(x.storageMap)
}

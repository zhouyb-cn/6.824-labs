package lock

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"time"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	lkey   string
	lvalue string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here
	lk.lkey = l
	lk.lvalue = kvtest.RandValue(8)
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		value, version, err := lk.ck.Get(lk.lkey)
		if err == rpc.ErrNoKey || (err == rpc.OK && value == "") {
			err = lk.ck.Put(lk.lkey, lk.lvalue, version)
			if err == rpc.OK {
				return
			}
			time.Sleep(10 * time.Millisecond)
			continue
		}
		// 有可能网络问题服务端已经处理过，判断value 直接返回
		if value == lk.lvalue {
			return
		}
	}
}

func (lk *Lock) Release() {
	// Your code here
	_, version, _ := lk.ck.Get(lk.lkey)
	lk.ck.Put(lk.lkey, "", version)
}

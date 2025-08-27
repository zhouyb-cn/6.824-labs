package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KV struct {
	v       string
	version rpc.Tversion
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	kv map[string]KV
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	// Your code here.
	kv.kv = make(map[string]KV)
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	key := args.Key
	value, ok := kv.kv[key]
	if !ok {
		reply.Err = rpc.ErrNoKey
		return
	}
	reply.Value = value.v
	reply.Version = value.version
	reply.Err = rpc.OK
	return
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	key := args.Key
	value := args.Value
	version := args.Version
	v, ok := kv.kv[key]
	if ok {
		if v.version == version {
			newValue := KV{
				v:       value,
				version: version + 1,
			}
			kv.kv[key] = newValue
			reply.Err = rpc.OK
			return
		} else {
			reply.Err = rpc.ErrVersion
			return
		}
	} else {
		if version == 0 {
			kv.kv[key] = KV{
				v:       value,
				version: 1,
			}
			reply.Err = rpc.OK
			return
		} else {
			reply.Err = rpc.ErrNoKey
			return
		}
	}
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}

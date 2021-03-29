package distkvs

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"
	"time"

	"github.com/DistributedClocks/tracing"
)

type StorageAddr string

// this matches the config file format in config/frontend_config.json
type FrontEndConfig struct {
	ClientAPIListenAddr  string
	StorageAPIListenAddr string
	Storage              StorageAddr
	TracerServerAddr     string
	TracerSecret         []byte
}

type FrontEndStorageStarted struct{}

type FrontEndStorageFailed struct{}

type FrontEndPut struct {
	Key   string
	Value string
}

type FrontEndPutResult struct {
	Err bool
}

type FrontEndGet struct {
	Key string
}

type FrontEndGetResult struct {
	Key   string
	Value *string
	Err   bool
}

type FrontEnd struct {
}

type RequestTask struct {
	mu			sync.Mutex
	requests	uint32
}

type StorageTasks struct {
	mu 		sync.Mutex
	tasks	map[string]*RequestTask
}

/** RPC Structs **/

type GetArgs struct {
	Key			string
	Token		tracing.TracingToken
}

type GetResult struct {
	Value		*string
	Err			bool
	RetToken	tracing.TracingToken
}

type PutArgs struct {
	Key			string
	Value		string
	Token		tracing.TracingToken
}

type PutResult struct {
	Err			bool
	RetToken	tracing.TracingToken
}

type ConnectArgs struct {
	StorageAddr		string	
}

type FrontEndRPCHandler struct {
	ftrace			*tracing.Tracer
	localTrace		*tracing.Trace
	storageTimeout 	uint8
	storage			*rpc.Client
	storageTasks	StorageTasks
}

/******************/

func (*FrontEnd) Start(clientAPIListenAddr string, storageAPIListenAddr string, storageTimeout uint8, ftrace *tracing.Tracer) error {
	trace := ftrace.CreateTrace()
	handler := &FrontEndRPCHandler{
		ftrace: ftrace,
		localTrace: trace,
		storageTimeout: storageTimeout,
		storageTasks: StorageTasks{
			tasks: make(map[string]*RequestTask),
		},
	}

	// register server
	server := rpc.NewServer()
	if err := server.Register(handler); err != nil {
		return fmt.Errorf("failed to register server: %s", err)
	}

	clientListener, err := net.Listen("tcp", clientAPIListenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %s", clientAPIListenAddr, clientListener)
	}

	// storageListener, err := net.Listen("tcp", storageAPIListenAddr)
	// if err != nil {
	// 	return fmt.Errorf("failed to listen on %s: %s", storageAPIListenAddr, storageListener)
	// }

	server.Accept(clientListener)
	// server.Accept(storageListener)

	return nil
}

func (f *FrontEndRPCHandler) Get(args GetArgs, reply *GetResult) error {
	req := f.storageTasks.get(args.Key)

	// lock
	req.acquire()

	trace := f.ftrace.ReceiveToken(args.Token)
	trace.RecordAction(FrontEndGet{Key: args.Key})

	callArgs := GetArgs{
		Key: args.Key,
		Token: trace.GenerateToken(),
	}
	result := GetResult{}

	// call storage
	err := f.storage.Call("StorageRPCHandler.Get", callArgs, &result)

	trace.Tracer.ReceiveToken(result.RetToken)
	if err != nil {
		log.Fatal("error occured while calling storage")
		return err
	}

	trace.RecordAction(FrontEndGetResult{
		Key: args.Key,
		Value: result.Value,
		Err: result.Err,
	})
	reply.Value = result.Value
	reply.Err = result.Err

	reply.RetToken = trace.GenerateToken()

	// unlock
	req.release()
	f.storageTasks.remove(args.Key)

	return nil
}

func (f *FrontEndRPCHandler) Put(args PutArgs, reply *PutResult) error {
	req := f.storageTasks.get(args.Key)

	// lock
	req.acquire()

	trace := f.ftrace.ReceiveToken(args.Token)
	trace.RecordAction(FrontEndPut{
		Key: args.Key,
		Value: args.Value,
	})

	callArgs := PutArgs{
		Key: args.Key,
		Value: args.Value,
		Token: trace.GenerateToken(),
	}
	result := PutResult{}

	// call storage
	call := f.storage.Go("StorageRPCHandler.Put", callArgs, &result, nil)

	var err error = nil
	select {
	case <- call.Done:
		trace.Tracer.ReceiveToken(result.RetToken)
		if call.Error != nil {
			err = call.Error
		}
	// set timeout
	case <- time.After(time.Duration(f.storageTimeout) * time.Second):
		trace.RecordAction(FrontEndPutResult{
			Err: true,
		})
		reply.Err = true
		// try put again
		err = f.storage.Call("StorageRPCHandler.Put", callArgs, &result)
	}

	if err != nil {
		log.Fatal("error occurred while calling storage")
		return err
	}

	trace.RecordAction(FrontEndPutResult{
		Err: result.Err,
	})
	reply.Err = result.Err
	reply.RetToken = trace.GenerateToken()

	// unlock
	req.release()
	f.storageTasks.remove(args.Key)

	return nil
}

func (f *FrontEndRPCHandler) Connect(args ConnectArgs, reply struct{}) error {
	storage, err := rpc.Dial("tcp", args.StorageAddr)

	if err != nil {
		return err
	}

	f.storage = storage

	return nil
}

func (s *StorageTasks) get(key string) *RequestTask {
	s.mu.Lock()
	defer s.mu.Unlock()
	val, ok := s.tasks[key];
	if !ok {
		val = &RequestTask{requests: 0}
		s.tasks[key] = val
	}

	return val
}

func (s *StorageTasks) remove(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if val, ok := s.tasks[key]; ok {
		if val.requests == 0 {
			delete(s.tasks, key)
		}
	}
}

func (r *RequestTask) acquire() {
	r.mu.Lock()
	r.requests += 1
}

func (r *RequestTask) release() {
	r.requests -= 1
	r.mu.Unlock()
}

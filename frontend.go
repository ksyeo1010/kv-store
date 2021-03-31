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
	Value		string
	Err			bool
	Found		bool
	RetToken	tracing.TracingToken
}

type GetStorageResult struct {
	Value 		string
	Found		bool
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

type PutStorageResult struct {
	RetToken	tracing.TracingToken
}

type ConnectArgs struct {
	StorageAddr		string	
}

type ConnectReply struct {}

type FrontEndRPCHandler struct {
	ftrace			*tracing.Tracer
	localTrace		*tracing.Trace
	storageTimeout 	uint8
	storage			*rpc.Client
	storageWaitCh	chan struct{}
	storageTasks	StorageTasks
}

/******************/

const NUM_RETRIES = 2

func (*FrontEnd) Start(clientAPIListenAddr string, storageAPIListenAddr string, storageTimeout uint8, ftrace *tracing.Tracer) error {
	trace := ftrace.CreateTrace()
	handler := &FrontEndRPCHandler{
		ftrace: ftrace,
		localTrace: trace,
		storageTimeout: storageTimeout,
		storageTasks: StorageTasks{
			tasks: make(map[string]*RequestTask),
		},
		storageWaitCh: make(chan struct{}),
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

	storageListener, err := net.Listen("tcp", storageAPIListenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %s", storageAPIListenAddr, storageListener)
	}

	go server.Accept(clientListener)
	server.Accept(storageListener)

	return nil
}

func (f *FrontEndRPCHandler) Get(args GetArgs, reply *GetResult) error {
	req := f.storageTasks.get(args.Key)

	// lock
	req.acquire()

	// wait for storage
	<- f.storageWaitCh

	trace := f.ftrace.ReceiveToken(args.Token)
	trace.RecordAction(FrontEndGet{Key: args.Key})

	callArgs := GetArgs{
		Key: args.Key,
		Token: trace.GenerateToken(),
	}
	result := GetStorageResult{}

	// call storage
	var is_err bool
	var value *string = nil

	callLoop:
	for i := 0; i < NUM_RETRIES; i++ {
		trace.RecordAction(FrontEndStorageStarted{})

		is_err = false
		// call storage
		callArgs.Token = trace.GenerateToken()
		call := f.storage.Go("StorageRPCHandler.Get", callArgs, &result, nil)

		select {
		case <- call.Done:
			trace.Tracer.ReceiveToken(result.RetToken)
			if call.Error == nil {
				if result.Found {
					value = &result.Value
				}
				break callLoop
			}
			log.Printf("error occured while calling storage: %s", call.Error.Error())
			is_err = true
		case <- time.After(time.Duration(f.storageTimeout) * time.Second):
			log.Printf("timeout occurred")
			is_err = true
		}

		trace.RecordAction(FrontEndStorageFailed{})
	}

	trace.RecordAction(FrontEndGetResult{
		Key: args.Key,
		Value: value,
		Err: is_err,
	})

	// reply
	reply.Value = result.Value
	reply.Err = is_err
	reply.Found = result.Found
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

	// wait for storage
	<- f.storageWaitCh

	trace := f.ftrace.ReceiveToken(args.Token)
	trace.RecordAction(FrontEndPut{
		Key: args.Key,
		Value: args.Value,
	})

	callArgs := PutArgs{
		Key: args.Key,
		Value: args.Value,
	}
	result := PutStorageResult{}

	var is_err bool

	callLoop:
	for i := 0; i < NUM_RETRIES; i++ {
		trace.RecordAction(FrontEndStorageStarted{})

		is_err = false
		// call storage
		callArgs.Token = trace.GenerateToken()
		call := f.storage.Go("StorageRPCHandler.Put", callArgs, &result, nil)

		select {
		case <- call.Done:
			if call.Error == nil {
				break callLoop
			}
			trace.Tracer.ReceiveToken(result.RetToken)
			log.Printf("error occurred while calling storage: %s", call.Error.Error())
			is_err = true
		case <- time.After(time.Duration(f.storageTimeout) * time.Second):
			log.Printf("timeout occurred")
			is_err = true
		}

		trace.RecordAction(FrontEndStorageFailed{})
	}
	
	trace.RecordAction(FrontEndPutResult{
		Err: is_err,
	})
	
	// reply
	reply.Err = is_err
	reply.RetToken = trace.GenerateToken()

	// unlock
	req.release()
	f.storageTasks.remove(args.Key)

	return nil
}

func (f *FrontEndRPCHandler) Connect(args ConnectArgs, reply *ConnectReply) error {
	storage, err := rpc.Dial("tcp", args.StorageAddr)
	if err != nil {
		return err
	}

	// close wait ch if it is open
	select{
	case <- f.storageWaitCh:
		// do nothing
	default:
		close(f.storageWaitCh)
	}

	log.Printf("storage connected on %s", args.StorageAddr)
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

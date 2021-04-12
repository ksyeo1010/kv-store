package distkvs

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"
	"time"

	"example.org/cpsc416/a5/wrapper"
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

type FrontEndStorageStarted struct {
	StorageID string
}

type FrontEndStorageFailed struct {
	StorageID string
}

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

type FrontEndStorageJoined struct {
	StorageIds []string
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
	Id			string
	StorageAddr
}

type ConnectReply struct {}

type StorageNode struct {
	mu              sync.Mutex
	nodes			map[string]*rpc.Client
}

type FrontEndRPCHandler struct {
	ftrace			*tracing.Tracer
	localTrace		*tracing.Trace
	storageTimeout 	uint8
	storageWaitCh	chan struct{}
	storageTasks	*StorageTasks
	storageNode     *StorageNode
}

/******************/

const NUM_RETRIES = 2

func (*FrontEnd) Start(clientAPIListenAddr string, storageAPIListenAddr string, storageTimeout uint8, ftrace *tracing.Tracer) error {
	trace := wrapper.CreateTrace(ftrace)
	handler := &FrontEndRPCHandler{
		ftrace: ftrace,
		localTrace: trace,
		storageTimeout: storageTimeout,
		storageTasks: &StorageTasks{
			tasks: make(map[string]*RequestTask),
		},
		storageWaitCh: make(chan struct{}),
		storageNode: &StorageNode{
			nodes: make(map[string]*rpc.Client),
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
	defer func () {
		req.release()
		f.storageTasks.remove(args.Key)
	}()

	// wait for any storage
	<- f.storageWaitCh

	trace := wrapper.ReceiveToken(f.ftrace, args.Token)
	wrapper.RecordAction(trace, FrontEndGet{Key: args.Key})

	// we call it this way so we can avoid strange behaviors
	// when a storage node is joining in the middle of computing
	nodes := f.storageNode.getNodes()

	if len(nodes) == 0 {
		wrapper.RecordAction(trace, FrontEndPutResult{
			Err: true,
		})

		// reply
		reply.Err = true
		reply.RetToken = wrapper.GenerateToken(trace)

		return nil
	}

	callArgs := GetArgs{
		Key: args.Key,
		Token: wrapper.GenerateToken(trace),
	}

	// result ch and cancel ch
	resultCh := make(chan *GetStorageResult)
	errorCh := make(chan struct{})

	for _, node := range nodes {
		go func(node *rpc.Client) {
			result := GetStorageResult{}
			for i := 0; i < NUM_RETRIES; i++ {
				err := node.Call("StorageRPCHandler.Get", callArgs, &result)
				if err != nil {
					resultCh <- &result
				}
				if i < NUM_RETRIES - 1 {
					time.Sleep(time.Duration(f.storageTimeout) * time.Second)
				}
			}

			// we have error once we reach here
			errorCh <- struct{}{}

			// TODO: add some failure logging
		}(node)
	}

	var is_err bool = true
	var value *string = nil
	var found bool = false

	resLoop:
	for i := 0; i < len(nodes); i++ {
		select{
		case <- errorCh:
			// do nothing
		case result := <- resultCh:
			is_err = false
			// we want to keep doing until we find some value in storage
			if result.Found {
				found = true
				value = &result.Value
				break resLoop
			}
		}
	}

	wrapper.RecordAction(trace, FrontEndGetResult{
		Key: args.Key,
		Value: value,
		Err: is_err,
	})

	// reply
	reply.Value = *value
	reply.Err = is_err
	reply.Found = found
	reply.RetToken = wrapper.GenerateToken(trace)

	return nil
}

func (f *FrontEndRPCHandler) Put(args PutArgs, reply *PutResult) error {
	req := f.storageTasks.get(args.Key)

	// lock
	req.acquire()
	defer func () {
		req.release()
		f.storageTasks.remove(args.Key)
	}()

	// wait for storage
	<- f.storageWaitCh

	trace := wrapper.ReceiveToken(f.ftrace, args.Token)
	wrapper.RecordAction(trace, FrontEndPut{
		Key: args.Key,
		Value: args.Value,
	})

	// we call it this way so we can avoid strange behaviors
	// when a storage node is joining in the middle of computing
	nodes := f.storageNode.getNodes()

	if len(nodes) == 0 {
		wrapper.RecordAction(trace, FrontEndPutResult{
			Err: true,
		})

		// reply
		reply.Err = true
		reply.RetToken = wrapper.GenerateToken(trace)

		return nil
	}

	callArgs := PutArgs{
		Key: args.Key,
		Value: args.Value,
		Token: wrapper.GenerateToken(trace),
	}

	// result ch
	resultCh := make(chan *PutStorageResult)
	errorCh := make(chan struct{})

	for _, node := range nodes {
		go func(node *rpc.Client) {
			result := PutStorageResult{}
			for i := 0; i < NUM_RETRIES; i++ {
				err := node.Call("StorageRPCHandler.Put", callArgs, &result)
				if err != nil {
					resultCh <- &result
				}
				if i < NUM_RETRIES - 1 {
					time.Sleep(time.Duration(f.storageTimeout) * time.Second)
				}
			}

			// we have error once we reach here
			errorCh <- struct{}{}

			// TODO: add some failure logging
		}(node)
	}

	var is_err bool = true

	resLoop:
	for i := 0; i < len(nodes); i++ {
		select{
		case <- errorCh:
			// do nothing
		case <- resultCh:
			is_err = false
			break resLoop
		}
	}

	wrapper.RecordAction(trace, FrontEndPutResult{
		Err: is_err,
	})
	
	// reply
	reply.Err = is_err
	reply.RetToken = wrapper.GenerateToken(trace)

	return nil
}

func (f *FrontEndRPCHandler) Connect(args ConnectArgs, reply *ConnectReply) error {
	storage, err := rpc.Dial("tcp", string(args.StorageAddr))
	if err != nil {
		return err
	}

	f.storageNode.add(f.localTrace, args.Id, storage)

	// close wait ch if it is open
	select{
	case <- f.storageWaitCh:
		// do nothing
	default:
		close(f.storageWaitCh)
	}

	log.Printf("storage connected on %s", args.StorageAddr)

	return nil
}

/** storage tasks implementations **/

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

/** Request Tasks implementations **/

func (r *RequestTask) acquire() {
	r.mu.Lock()
	r.requests += 1
}

func (r *RequestTask) release() {
	r.requests -= 1
	r.mu.Unlock()
}

/** Storage node implementations **/

func (s *StorageNode) getNodes() map[string]*rpc.Client {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.nodes
}

func (s *StorageNode) add(trace *tracing.Trace, id string, storage *rpc.Client) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// we say if storage failed and rejoined while front end
	// did not know, we are fine with it
	if _, ok := s.nodes[id]; ok {
		return
	}

	s.nodes[id] = storage
	wrapper.RecordAction(trace, FrontEndStorageStarted{StorageID: id})

	// get all keys
	keys := make([]string, len(s.nodes))
	for k := range s.nodes {
		keys = append(keys, k)
	}

	wrapper.RecordAction(trace, FrontEndStorageJoined{StorageIds: keys})
}

func (s *StorageNode) remove(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	if node, ok := s.nodes[id]; ok {
		node.Close()
		delete(s.nodes, id)
		return
	}

	log.Fatalf("Tried to remove nonexisting id %s.", id)
}

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
	mu 			sync.Mutex
	tasks		map[string]*RequestTask
}

type StorageNodes struct {
	mu              sync.Mutex
	nodes			map[string]*StorageNode
}

type StorageNode struct {
	client            *rpc.Client
	joined          bool
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

type JoinedArgs struct {
	StorageID 	string
}

type JoinedReply struct {

}

type FrontEndRPCHandler struct {
	ftrace			*tracing.Tracer
	localTrace		*tracing.Trace
	storageTimeout 	uint8
	storageTasks	*StorageTasks
	storageNodes    *StorageNodes
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
		storageNodes: &StorageNodes{
			nodes: make(map[string]*StorageNode),
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

	trace := wrapper.ReceiveToken(f.ftrace, args.Token)
	wrapper.RecordAction(trace, FrontEndGet{Key: args.Key})

	callArgs := GetArgs{
		Key: args.Key,
		Token: wrapper.GenerateToken(trace),
	}

	// result ch and cancel ch
	resultCh := make(chan *GetStorageResult)
	errorCh := make(chan struct{})

	// we call it this way (only once in whole function) so we can avoid data race
	// when a storage node is joining in the middle of computing
	nodes := f.storageNodes.getNodes()
	for id, node := range nodes {
		go func(id string, node *StorageNode) {
			result := GetStorageResult{}
			for i := 0; i < NUM_RETRIES; i++ {
				err := node.client.Call("StorageRPCHandler.Get", callArgs, &result)
				if err == nil {
					wrapper.ReceiveToken(trace.Tracer, result.RetToken)
					if node.joined {
						resultCh <- &result
					}
					return
				}
				if i < NUM_RETRIES - 1 {
					time.Sleep(time.Duration(f.storageTimeout) * time.Second)
				}
			}
			// remove since it is a failed one
			f.storageNodes.remove(trace, id)
			// we have error once we reach here
			errorCh <- struct{}{}
		}(id, node)
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
			found = true
			value = &result.Value
			break resLoop
		}
	}

	wrapper.RecordAction(trace, FrontEndGetResult{
		Key: args.Key,
		Value: value,
		Err: is_err,
	})

	// reply
	if value != nil {
		reply.Value = *value
	}
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

	trace := wrapper.ReceiveToken(f.ftrace, args.Token)
	wrapper.RecordAction(trace, FrontEndPut{
		Key: args.Key,
		Value: args.Value,
	})

	callArgs := PutArgs{
		Key: args.Key,
		Value: args.Value,
		Token: wrapper.GenerateToken(trace),
	}

	// result ch
	resultCh := make(chan *PutStorageResult)
	errorCh := make(chan struct{})

	// we call it this way (only once in whole function) so we can avoid data race
	// when a storage node is joining in the middle of computing
	nodes := f.storageNodes.getNodes()
	for id, node := range nodes {
		go func(id string, node *StorageNode) {
			result := PutStorageResult{}
			for i := 0; i < NUM_RETRIES; i++ {
				err := node.client.Call("StorageRPCHandler.Put", callArgs, &result)
				if err == nil {
					wrapper.ReceiveToken(trace.Tracer, result.RetToken)
					if node.joined {
						resultCh <- &result
					}
					return
				}
				if i < NUM_RETRIES - 1 {
					time.Sleep(time.Duration(f.storageTimeout) * time.Second)
				}
			}
			// remove since it is a failure
			f.storageNodes.remove(trace, id)
			// we have error once we reach here
			errorCh <- struct{}{}
		}(id, node)
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
	
	f.storageNodes.add(f.localTrace, args.Id, storage)
	go f.initializeStorage(args.Id, storage)

	log.Printf("%s connected on %s", args.Id, args.StorageAddr)

	return nil
}

func (f *FrontEndRPCHandler) initializeStorage(argsId string, storage *rpc.Client) {
	nodes := f.storageNodes.getNodes()

	var state string
	var useExistingState bool = true

	result := StorageStateReply{}
	for id, node := range nodes {
		if argsId == id || !node.joined {
			continue
		}
		// try to get first storage state that works
		err := node.client.Call("StorageRPCHandler.State", struct{}{}, &result)
		if err == nil {
			state = result.State
			useExistingState = false
			break
		}
	}

	// if we reach here its the only one alive.
	args := StorageInitializeArgs{
		State: state,
		UseExistingState: useExistingState,
	}
	err := storage.Call("StorageRPCHandler.Initialize", args, nil)
	if err == nil {
		f.storageNodes.storageNodeJoined(f.localTrace, argsId)
	}
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

func (s *StorageNodes) getNodes() map[string]*StorageNode {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.nodes
}

func (s *StorageNodes) add(trace *tracing.Trace, id string, storage *rpc.Client) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if node, ok := s.nodes[id]; ok {
		node.client.Close()
	} else {
		wrapper.RecordAction(trace, FrontEndStorageStarted{StorageID: id})
	}

	s.nodes[id] = &StorageNode{
		client: storage,
		joined: false,
	}
}

func (s *StorageNodes) storageNodeJoined(trace *tracing.Trace, id string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	node, ok := s.nodes[id]

	if ok {
		node.joined = true

		keys := s.getJoinedStorageNodes()
		wrapper.RecordAction(trace, FrontEndStorageJoined{StorageIds: keys})
		return
	}

	log.Fatalf("Tried to update storage node to joined that doesn't exist %s.", id)
}

func (s *StorageNodes) remove(trace *tracing.Trace, id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	if node, ok := s.nodes[id]; ok {
		node.client.Close()
		wrapper.RecordAction(trace, FrontEndStorageFailed{StorageID: id})
		delete(s.nodes, id)

		keys := s.getJoinedStorageNodes()
		wrapper.RecordAction(trace, FrontEndStorageJoined{StorageIds: keys})
		return
	}

	log.Printf("Tried to remove nonexisting id %s.", id)
}

func (s *StorageNodes) getJoinedStorageNodes() []string {
	keys := make([]string, 0)
	for k, node := range s.nodes {
		if node.joined {
			keys = append(keys, k)
		}
	}

	return keys
}

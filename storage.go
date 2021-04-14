package distkvs

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"

	"example.org/cpsc416/a5/wrapper"
	"github.com/DistributedClocks/tracing"
)

type StorageConfig struct {
	StorageID        string
	StorageAdd       StorageAddr
	ListenAddr       string
	FrontEndAddr     string
	DiskPath         string
	TracerServerAddr string
	TracerSecret     []byte
}

type StorageLoadSuccess struct {
	StorageID string
	State     map[string]string
}

type StoragePut struct {
	StorageID string
	Key       string
	Value     string
}

type StorageSaveData struct {
	StorageID string
	Key       string
	Value     string
}

type StorageGet struct {
	StorageID string
	Key       string
}

type StorageGetResult struct {
	StorageID string
	Key       string
	Value     *string
}

type StorageJoining struct {
	StorageID string
}

type StorageJoined struct {
	StorageID string
	State     map[string]string
}

type StorageGetArgs struct {
	Key				string
	Token 			tracing.TracingToken
}

type StorageGetReply struct {
	Value			string
	Found			bool
	RetToken 		tracing.TracingToken
}

type StoragePutArgs struct {
	Key				string
	Value           string
	Token 			tracing.TracingToken
}

type StoragePutReply struct {
	RetToken 		tracing.TracingToken
}

type StorageStateReply struct {
	State 	  		string
}

type StorageInitializeArgs struct {
	State   		string
	UseExistingState bool
}

type StorageInitializeReply struct {

}

type Storage struct {
	frontEndAddr    string
	storageAddr     string
	frontEnd 		*rpc.Client
	diskPath        string
	filePath        string
	tracer			*tracing.Tracer
	memory			*Memory
	id              string
}

type StorageRPCHandler struct {
	id				string
	tracer          *tracing.Tracer
	localTrace		*tracing.Trace
	memory			*Memory
	filePath        string
	mu              sync.Mutex
	joiningQueue    *JoiningQueue
}

func (s *Storage) Start(storageId string, frontEndAddr string, storageAddr string, diskPath string, strace *tracing.Tracer) error {
	// create RPC connection to frontend
	frontEnd, err := rpc.Dial("tcp", frontEndAddr)
	if err != nil {
		return fmt.Errorf("error connecting with frontend: %s", err)
	}

	// initialize properties
	s.frontEndAddr = frontEndAddr
	s.storageAddr = storageAddr
	s.frontEnd = frontEnd
	s.diskPath = diskPath
	s.filePath = diskPath + "storage.json"
	s.tracer = strace
	s.memory = NewMemory()
	s.id = storageId


	trace := wrapper.CreateTrace(strace)

	// initialize RPC
	err = s.initializeRPC(s.id, trace)
	if err != nil {
		return err
	}

	// read storage from disk into memory
	err = s.readStorage(trace)
	if err != nil {
		return fmt.Errorf("error reading storage from disk: %s", err)
	}

	wrapper.RecordAction(trace, StorageJoining{
		StorageID: s.id,
	})

	// call frontend with port
	s.frontEnd.Call("FrontEndRPCHandler.Connect", ConnectArgs{Id: s.id, StorageAddr: StorageAddr(storageAddr)}, &ConnectReply{})

	// infinitely wait
	stop := make(chan struct{})
	<- stop

	return nil
}

func (s *Storage) initializeRPC(id string, trace *tracing.Trace) error {
	server := rpc.NewServer()
	var joiningQueue = &JoiningQueue{
		queue: 			make([]*StoragePutArgs, 0),
		isJoining: 		true,
	}
	err := server.Register(&StorageRPCHandler{
		id:				id,
		tracer: 		s.tracer,
		localTrace: 	trace,
		memory:     	s.memory,
		filePath:   	s.filePath,
		joiningQueue: 	joiningQueue,
	})

	if err != nil {
		return fmt.Errorf("format of storage RPC is not correct: %s", err)
	}

	listener, e := net.Listen("tcp", s.storageAddr)
	if e != nil {
		return fmt.Errorf("listen error: %s", e)
	}

	log.Printf("Serving Storage RPCs on port %s", s.storageAddr)
	go server.Accept(listener)

	return nil
}

// readStorage loads disk into memory
func (s *Storage) readStorage(trace *tracing.Trace) error {
	// check if file exists
	if _, err := os.Stat(s.filePath); os.IsNotExist(err) {
		// create file if it doesn't exist
		emptyJson := []byte("{\n}\n")
		err := ioutil.WriteFile(s.filePath, emptyJson, 0644)
		if err != nil {
			return err
		}
		wrapper.RecordAction(trace, StorageLoadSuccess{
			StorageID: s.id,
			State: make(map[string]string),
		})

		return nil
	}

	// open file
	jsonFile, err := os.Open(s.filePath)
	if err != nil {
		return err
	}
	defer jsonFile.Close()

	// parse file
	byteValue, _ := ioutil.ReadAll(jsonFile)
	var keyValuePairs map[string]string
	err = json.Unmarshal(byteValue, &keyValuePairs)
	if err != nil {
		return err
	}

	// load into memory
	s.memory.Load(keyValuePairs)

	wrapper.RecordAction(trace, StorageLoadSuccess{
		StorageID: s.id,
		State: keyValuePairs,
	})

	return nil
}

// Get is a blocking async RPC from the Frontend
// fetching the value from memory
func (s *StorageRPCHandler) Get(args StorageGetArgs, reply *StorageGetReply) error {
	trace := wrapper.ReceiveToken(s.tracer, args.Token)

	wrapper.RecordAction(trace, StorageGet{
		StorageID: s.id,
		Key: args.Key,
	})

	var value *string = nil
	if !s.joiningQueue.getJoining() {
		value = s.memory.Get(args.Key)

		if value != nil {
			reply.Value = *value
			reply.Found = true
		}
	}


	wrapper.RecordAction(trace, StorageGetResult{
		StorageID: s.id,
		Key: args.Key,
		Value: value,
	})
	
	reply.RetToken = wrapper.GenerateToken(trace)
	
	return nil
}

// Put is a blocking async RPC from the Frontend
// saving a new value to storage and memory
func (s *StorageRPCHandler) Put(args StoragePutArgs, reply *StoragePutReply) error {
	trace := wrapper.ReceiveToken(s.tracer, args.Token)

	wrapper.RecordAction(trace, StoragePut{
		StorageID: s.id,
		Key: args.Key,
		Value: args.Value,
	})

	if !s.joiningQueue.getJoining() {
		err := s.updateKVS(args.Key, args.Value)
		if err != nil {
			return err
		}
	} else {
		s.joiningQueue.addToQueue(&args)
	}

	wrapper.RecordAction(trace, StorageSaveData{
		StorageID: s.id,
		Key: args.Key,
		Value: args.Value,
	})

	reply.RetToken = wrapper.GenerateToken(trace)

	return nil
}

func (s *StorageRPCHandler) State(args struct{}, reply *StorageStateReply) error {
	jsonString, err := json.Marshal(s.memory.GetAll())
	if err != nil {
		return err
	}
	reply.State = string(jsonString)
	return nil
}

func (s *StorageRPCHandler) Initialize(args StorageInitializeArgs, reply *StorageInitializeReply) error {
	if !args.UseExistingState {
		var keyValuePairs map[string]string
		err := json.Unmarshal([]byte(args.State), &keyValuePairs)
		if err != nil {
			return err
		}
		// update KVS
		s.overwriteKVS(keyValuePairs)

		// load into memory
		s.memory.Load(keyValuePairs)
	}

	//process put requests
	var item = s.joiningQueue.popFromQueue(s.id, s.localTrace, s.memory.GetAll())
	for item != nil {
		s.updateKVS(item.Key, item.Value)
		item = s.joiningQueue.popFromQueue(s.id, s.localTrace, s.memory.GetAll())
	}

	return nil
}

func (s *StorageRPCHandler) overwriteKVS(state map[string]string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// open file
	jsonFile, err := os.Open(s.filePath)
	if err != nil {
		return err
	}
	defer jsonFile.Close()

	// rewrite file
	file, err := json.Marshal(state)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(s.filePath, file, 0644)
	if err != nil {
		return err
	}

	return nil
}

func (s *StorageRPCHandler) updateKVS(key string, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// open file
	jsonFile, err := os.Open(s.filePath)
	if err != nil {
		return err
	}
	defer jsonFile.Close()

	// parse file
	byteValue, _ := ioutil.ReadAll(jsonFile)
	var keyValuePairs map[string]string
	err = json.Unmarshal(byteValue, &keyValuePairs)
	if err != nil {
		return err
	}

	// update key-value in json
	keyValuePairs[key] = value

	// rewrite file
	file, err := json.Marshal(keyValuePairs)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(s.filePath, file, 0644)
	if err != nil {
		return err
	}

	s.memory.Put(key, value)

	return nil
}

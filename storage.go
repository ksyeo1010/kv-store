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

type StorageInitializeArgs struct {
	State     map[string]string
	useExistingState bool
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
	tracer          *tracing.Tracer
	memory			*Memory
	filePath        string
	mu              sync.Mutex
	isJoining 		bool
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

	// initialize RPC
	err = s.initializeRPC()
	if err != nil {
		return err
	}

	// read storage from disk into memory
	trace := wrapper.CreateTrace(strace)
	err = s.readStorage(trace)
	if err != nil {
		return fmt.Errorf("error reading storage from disk: %s", err)
	}

	wrapper.RecordAction(trace, StorageJoining{
		StorageID: s.id,
	})

	// call frontend with port
	s.frontEnd.Call("FrontEndRPCHandler.Connect", ConnectArgs{StorageAddr: StorageAddr(storageAddr)}, &ConnectReply{})

	// infinitely wait
	stop := make(chan struct{})
	<- stop

	return nil
}

func (s *Storage) initializeRPC() error {
	server := rpc.NewServer()
	err := server.Register(&StorageRPCHandler{
		tracer: 	s.tracer,
		memory:     s.memory,
		filePath:   s.filePath,
		isJoining:    true,
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
		State: keyValuePairs,
	})

	return nil
}

// Get is a blocking async RPC from the Frontend
// fetching the value from memory
func (s *StorageRPCHandler) Get(args StorageGetArgs, reply *StorageGetReply) error {
	trace := wrapper.ReceiveToken(s.tracer, args.Token)

	wrapper.RecordAction(trace, StorageGet{
		Key: args.Key,
	})

	value := s.memory.Get(args.Key)

	wrapper.RecordAction(trace, StorageGetResult{
		Key: args.Key,
		Value: value,
	})

	if value != nil {
		reply.Value = *value
		reply.Found = true
	}
	
	reply.RetToken = wrapper.GenerateToken(trace)
	
	return nil
}

// Put is a blocking async RPC from the Frontend
// saving a new value to storage and memory
func (s *StorageRPCHandler) Put(args StoragePutArgs, reply *StoragePutReply) error {
	trace := wrapper.ReceiveToken(s.tracer, args.Token)

	wrapper.RecordAction(trace, StoragePut{
		Key: args.Key,
		Value: args.Value,
	})

	err := s.updateKVS(args.Key, args.Value)
	if err != nil {
		return err
	}

	wrapper.RecordAction(trace, StorageSaveData{
		Key: args.Key,
		Value: args.Value,
	})

	reply.RetToken = wrapper.GenerateToken(trace)

	return nil
}

//
func (s *StorageRPCHandler) Initialize(args StorageInitializeArgs, reply *StorageInitializeReply) error {
	if !args.useExistingState {
		// update KVS
		s.overwriteKVS(args.State)

		// load into memory
		s.memory.Load(args.State)
	}

	s.updateJoined()

	//process put requests
	//send request to frontend saying joined

	return nil
}

func (s *StorageRPCHandler) overwriteKVS(state map[string]string) error {
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

func (s *StorageRPCHandler) getJoining() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.isJoining
}

func (s *StorageRPCHandler) updateJoined() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.isJoining = false
}

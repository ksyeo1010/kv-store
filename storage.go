package distkvs

import (
	"encoding/json"
	"fmt"
	"github.com/DistributedClocks/tracing"
	"io/ioutil"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
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
	State map[string]string
}

type StoragePut struct {
	Key   string
	Value string
}

type StorageSaveData struct {
	Key   string
	Value string
}

type StorageGet struct {
	Key string
}

type StorageGetResult struct {
	Key   string
	Value *string
}

type StorageGetArgs struct {
	Key				string
	Token 			tracing.TracingToken
}

type StorageGetReply struct {
	Value			*string
	Token 			tracing.TracingToken
}

type StoragePutArgs struct {
	Key				string
	Value           string
	Token 			tracing.TracingToken
}

type StoragePutReply struct {
	Token 			tracing.TracingToken
	Error 			error
}

type Storage struct {
	frontEndAddr    string
	storageAddr     string
	frontEnd 		*rpc.Client
	diskPath        string
	filePath        string
	tracer			*tracing.Tracer
	memory			*Memory
}

type StorageRPCHandler struct {
	tracer          *tracing.Tracer
	memory			*Memory
	filePath        string
	mu              sync.Mutex
}

func (s *Storage) Start(frontEndAddr string, storageAddr string, diskPath string, strace *tracing.Tracer) error {
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

	// initialize RPC
	err = s.initializeRPC()
	if err != nil {
		return err
	}

	// read storage from disk into memory
	trace := s.tracer.CreateTrace()
	err = s.readStorage(trace)
	if err != nil {
		return fmt.Errorf("error reading storage from disk: %s", err)
	}

	// call frontend with port
	s.frontEnd.Call("FrontEndRPCHandler.Connect", ConnectArgs{StorageAddr: storageAddr}, &ConnectReply{})

	return nil
}

func (s *Storage) initializeRPC() error {
	server := rpc.NewServer()
	err := server.Register(&StorageRPCHandler{
		tracer: 	s.tracer,
		memory:     s.memory,
		filePath:   s.filePath,
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
		trace.RecordAction(StorageLoadSuccess{
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

	trace.RecordAction(StorageLoadSuccess{
		State: keyValuePairs,
	})

	return nil
}

// Get is a blocking async RPC from the Frontend
// fetching the value from memory
func (s *StorageRPCHandler) Get(args StorageGetArgs, reply *StorageGetReply) {
	trace := s.tracer.ReceiveToken(args.Token)

	trace.RecordAction(StorageGet{
		Key: args.Key,
	})

	value := s.memory.Get(args.Key)

	trace.RecordAction(StorageGetResult{
		Key: args.Key,
		Value: value,
	})

	reply.Value = value
	reply.Token = trace.GenerateToken()
}

// Put is a blocking async RPC from the Frontend
// saving a new value to storage and memory
func (s *StorageRPCHandler) Put(args StoragePutArgs, reply *StoragePutReply) {
	trace := s.tracer.ReceiveToken(args.Token)

	trace.RecordAction(StoragePut{
		Key: args.Key,
		Value: args.Value,
	})

	err := s.updateKVS(args.Key, args.Value)

	trace.RecordAction(StorageSaveData{
		Key: args.Key,
		Value: args.Value,
	})

	reply.Token = trace.GenerateToken()
	reply.Error = err
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

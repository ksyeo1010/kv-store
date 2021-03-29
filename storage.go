package distkvs

import (
	"encoding/json"
	"fmt"
	"github.com/DistributedClocks/tracing"
	"io/ioutil"
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
	mu               sync.Mutex
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

	s.readStorage()
	return nil
}

// readStorage loads disk into memory
func (s *Storage) readStorage() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// check if file exists
	if _, err := os.Stat(s.filePath); os.IsNotExist(err) {
		// create file if it doesn't exist
		emptyJson := []byte("{\n}\n")
		err := ioutil.WriteFile(s.filePath, emptyJson, 0644)
		if err != nil {
			return err
		}
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

	return nil
}

// Get is a blocking async RPC from the Frontend
// fetching the value from memory
func (s *Storage) Get(args StorageGetArgs, reply *StorageGetReply) {
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
func (s *Storage) Put(args StoragePutArgs, reply *StoragePutReply) {
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

func (s *Storage) updateKVS(key string, value string) error {
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

package distkvs

import (
	"fmt"
	"github.com/DistributedClocks/tracing"
	"net/rpc"
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
}

type Storage struct {
	frontEndAddr    string
	storageAddr     string
	frontEnd 		*rpc.Client
	diskPath        string
	tracer			*tracing.Tracer
	memory			*Memory
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
	s.tracer = strace
	s.memory = NewMemory()

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

	s.memory.Put(args.Key, args.Value)
	//TODO: Save to disk

	trace.RecordAction(StorageSaveData{
		Key: args.Key,
		Value: args.Value,
	})

	reply.Token = trace.GenerateToken()
}


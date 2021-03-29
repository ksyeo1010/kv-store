package distkvs

import (
	"fmt"
	"net"
	"net/rpc"

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

/** RPC Structs **/

type ClientGetArgs struct {
	Key			string
	Token		tracing.TracingToken
}

type ClientGetResult struct {
	Value		*string
	Err			bool
	RetToken	tracing.TracingToken
}

type ClientPutArgs struct {
	Key			string
	Value		string
	Token		tracing.TracingToken
}

type ClientPutResult struct {
	Err			bool
	RetToken	tracing.TracingToken
}

type FrontEndRPCHandler struct {
	ftrace		*tracing.Tracer
	localTrace	*tracing.Trace
}

/******************/

func (*FrontEnd) Start(clientAPIListenAddr string, storageAPIListenAddr string, storageTimeout uint8, ftrace *tracing.Tracer) error {
	trace := ftrace.CreateTrace()
	handler := &FrontEndRPCHandler{
		ftrace: ftrace,
		localTrace: trace,
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

func (f *FrontEndRPCHandler) Get(args ClientGetArgs, reply *ClientGetResult) error {
	trace := f.ftrace.ReceiveToken(args.Token)

	reply.Value = nil
	reply.Err = true
	reply.RetToken = trace.GenerateToken()

	return nil
}

func (f *FrontEndRPCHandler) Put(args ClientPutArgs, reply *ClientPutResult) error {
	trace := f.ftrace.ReceiveToken(args.Token)

	reply.Err = true
	reply.RetToken = trace.GenerateToken()

	return nil
}

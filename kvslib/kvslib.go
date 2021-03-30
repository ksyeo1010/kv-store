// Package kvslib provides an API which is a wrapper around RPC calls to the
// frontend.
package kvslib

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"

	"github.com/DistributedClocks/tracing"
)

type KvslibBegin struct {
	ClientId string
}

type KvslibPut struct {
	ClientId string
	OpId     uint32
	Key      string
	Value    string
}

type KvslibGet struct {
	ClientId string
	OpId     uint32
	Key      string
}

type KvslibPutResult struct {
	OpId uint32
	Err  bool
}

type KvslibGetResult struct {
	OpId  uint32
	Key   string
	Value *string
	Err   bool
}

type KvslibComplete struct {
	ClientId string
}

// NotifyChannel is used for notifying the client about a mining result.
type NotifyChannel chan ResultStruct

// CloseChannel is used for notifying a ksvlib action Close event.
type CloseChannel chan struct{}

type ResultStruct struct {
	OpId        uint32
	StorageFail bool
	Result      *string
}

type KVS struct {
	clientId		string
	opId     		uint32
	frontEndAddr	string
	frontEnd 		*rpc.Client
	notifyCh 		NotifyChannel
	closeCh  		CloseChannel
	closeWg  		*sync.WaitGroup
	localTrace		*tracing.Trace
}

func NewKVS() *KVS {
	return &KVS{
		clientId: 		"",	
		opId: 	  		0,
		frontEndAddr: 	"",
		frontEnd: 		nil,
		notifyCh: 		nil,
		closeCh:  		nil,
		closeWg:  		nil,
		localTrace: 	nil,
	}
}

/** RPC structs **/

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

// Initialize Initializes the instance of KVS to use for connecting to the frontend,
// and the frontends IP:port. The returned notify-channel channel must
// have capacity ChCapacity and must be used by kvslib to deliver all solution
// notifications. If there is an issue with connecting, this should return
// an appropriate err value, otherwise err should be set to nil.
func (d *KVS) Initialize(localTracer *tracing.Tracer, clientId string, frontEndAddr string, chCapacity uint) (NotifyChannel, error) {
	frontEnd, err := rpc.Dial("tcp", frontEndAddr)
	if err != nil {
		return nil, fmt.Errorf("error connecting with frontend: %s", err)
	}
	// save addr
	d.frontEndAddr = frontEndAddr

	// create local tracer
	d.clientId = clientId
	d.localTrace = localTracer.CreateTrace()
	d.localTrace.RecordAction(KvslibBegin{ClientId: clientId})
	d.frontEnd = frontEnd

	// create channels
	d.notifyCh = make(NotifyChannel, chCapacity)
	d.closeCh = make(CloseChannel, chCapacity)

	var wg sync.WaitGroup
	d.closeWg = &wg

	return d.notifyCh, nil
}

// Get is a non-blocking request from the client to the system. This call is used by
// the client when it wants to get value for a key.
func (d *KVS) Get(tracer *tracing.Tracer, clientId string, key string) (uint32, error) {
	trace := tracer.CreateTrace()
	// add to wg
	d.closeWg.Add(1)

	if err := d.checkConn(); err != nil {
		return 0, err
	}

	opId := d.getOpId()
	go d.callGet(trace, clientId, opId, key)

	return opId, nil
}

// Put is a non-blocking request from the client to the system. This call is used by
// the client when it wants to update the value of an existing key or add add a new
// key and value pair.
func (d *KVS) Put(tracer *tracing.Tracer, clientId string, key string, value string) (uint32, error) {
	trace := tracer.CreateTrace()
	// add to wg
	d.closeWg.Add(1)

	if err := d.checkConn(); err != nil {
		return 0, err
	}

	opId := d.getOpId()
	go d.callPut(trace, clientId, opId, key, value)

	return opId, nil
}

func (d *KVS) callGet(trace *tracing.Trace, clientId string, opId uint32, key string) {
	defer func() {
		d.closeWg.Done()
	}()

	trace.RecordAction(KvslibGet{
		ClientId: clientId,
		OpId: opId,
		Key: key,
	})

	args := GetArgs{
		Key: key,
		Token: trace.GenerateToken(),
	}
	result := GetResult{}
	call := d.frontEnd.Go("FrontEndRPCHandler.Get", args, &result, nil)
	for {
		select {
		case <-call.Done:
			if call.Error != nil {
				log.Fatal(call.Error)
			} else {
				trace.Tracer.ReceiveToken(result.RetToken)
				trace.RecordAction(KvslibGetResult{
					OpId: opId,
					Key: key,
					Value: result.Value,
					Err: result.Err,
				})
				// Assume value is nil if Err occurred
				d.notifyCh <- ResultStruct{
					OpId: opId,
					StorageFail: result.Err,
					Result: result.Value,
				}
			}
			return
		case <- d.closeCh:
			log.Printf("cancel callGet")
			return
		}
	}
}

func (d *KVS) callPut(trace *tracing.Trace, clientId string, opId uint32, key string, value string) {
	defer func() {
		d.closeWg.Done()
	}()

	trace.RecordAction(KvslibPut{
		ClientId: clientId,
		OpId: opId,
		Key: key,
		Value: value,
	})

	args := PutArgs{
		Key: key,
		Value: value,
		Token: trace.GenerateToken(),
	}
	result := PutResult{}
	call := d.frontEnd.Go("FrontEndRPCHandler.Put", args, &result, nil)
	for {
		select {
		case <-call.Done:
			trace.Tracer.ReceiveToken(result.RetToken)
			if (call.Error != nil) {
				log.Fatal(call.Error)
			} else {
				trace.RecordAction(KvslibPutResult{
					OpId: opId,
					Err: result.Err,
				})
				var resVal *string = nil
				// check if it was error
				if !result.Err {
					resVal = &value
				}
				d.notifyCh <- ResultStruct{
					OpId: opId,
					StorageFail: result.Err,
					Result: resVal,
				}
			}
			return
		case <- d.closeCh:
			log.Printf("cancel callPut")
			return
		}
	}
}

// Close Stops the KVS instance from communicating with the frontend and
// from delivering any solutions via the notify-channel. If there is an issue
// with stopping, this should return an appropriate err value, otherwise err
// should be set to nil.
func (d *KVS) Close() error {
	// notify all goroutines to close
	close(d.closeCh)
	d.closeWg.Wait()

	// on close we log
	d.localTrace.RecordAction(KvslibComplete{ClientId: d.clientId})

	// close frontend
	if err := d.frontEnd.Close(); err != nil {
		return err
	}
	d.frontEnd = nil

	return nil
}

func (d *KVS) checkConn() error {
	_, err := net.Dial("tcp", d.frontEndAddr)
	return err
}

// getOpId gets the next operation ID
func (d *KVS) getOpId() uint32 {
	defer func() {
		d.opId += 1
	}()
	return d.opId
}

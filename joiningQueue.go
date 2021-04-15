package distkvs

import (
	"example.org/cpsc416/a6/wrapper"
	"github.com/DistributedClocks/tracing"
	"sync"
)

type JoiningQueue struct {
	mu 				sync.Mutex
	queue 			[]*StoragePutArgs
	isJoining       bool
}

func (q *JoiningQueue) addToQueue(item *StoragePutArgs) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.queue = append(q.queue, item)
}

func (q *JoiningQueue) popFromQueue(storageID string, trace *tracing.Trace, state map[string]string) *StoragePutArgs {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.queue) > 0 {
		var item = q.queue[0]
		q.queue = q.queue[1:]

		return item
	} else {
		wrapper.RecordAction(trace, StorageJoined{
			StorageID: storageID,
			State: state,
		})

		q.isJoining = false
		return nil
	}

}

func (q *JoiningQueue) getJoining() bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	return q.isJoining
}
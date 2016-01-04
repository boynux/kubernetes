package consul
import (
	"k8s.io/kubernetes/pkg/runtime"
	"k8s.io/kubernetes/pkg/storage"
	consulapi "github.com/hashicorp/consul/api"
	"golang.org/x/net/context"
	"k8s.io/kubernetes/pkg/watch"
	"sync"
	"time"
	"k8s.io/kubernetes/pkg/util"
	"k8s.io/kubernetes/pkg/api/unversioned"
	"net/http"
	"github.com/golang/glog"
	"sync/atomic"
	"fmt"
	"k8s.io/kubernetes/_tmp/pkg/api"
)

const (
// DefaultWatchWaitTime is how long we block for at a
// time to check if the watched key has changed. This
// affects the minimum time it takes to cancel a watch.
	DefaultWatchWaitTime = 15 * time.Hour
)

// Consul watch event actions
type ConsulAction string

const (
	ConsulCreate = "create"
	ConsulSet    = "set"
	EtcdCAS    	 = "compareAndSwap"
	ConsulDelete = "delete"
)

type ConsulResponse struct {
	// Action is the name of the operation that occurred. Possible values
	// include get, set, delete, update, create, compareAndSwap,
	// compareAndDelete and expire.
	Action ConsulAction `json:"action"`

	// Node represents the state of the relevant etcd Node.
	Node *consulapi.KVPair `json:"node"`

	// Index holds the cluster-level index at the time the Response was generated.
	// This index is not tied to the Node(s) contained in this Response.
	Index uint64 `json:"-"`
}


// HighWaterMark is a thread-safe object for tracking the maximum value seen
// for some quantity.
type HighWaterMark int64

// Update returns true if and only if 'current' is the highest value ever seen.
func (hwm *HighWaterMark) Update(current int64) bool {
	for {
		old := atomic.LoadInt64((*int64)(hwm))
		if current <= old {
			return false
		}
		if atomic.CompareAndSwapInt64((*int64)(hwm), old, current) {
			return true
		}
	}
}


// TransformFunc attempts to convert an object to another object for use with a watcher.
type TransformFunc func(runtime.Object) (runtime.Object, error)

// includeFunc returns true if the given key should be considered part of a watch
type includeFunc func(key string) bool

// exceptKey is an includeFunc that returns false when the provided key matches the watched key
func exceptKey(except string) includeFunc {
	return func(key string) bool {
		return key != except
	}
}

// consulWatcher converts a native consul watch to a watch.Interface.
type consulWatcher struct {
	encoding  runtime.Codec
	versioner storage.Versioner
	transform TransformFunc

	list    bool // If we're doing a recursive watch, should be true.
	include includeFunc
	filter  storage.FilterFunc

	consulIncoming  chan *ConsulResponse
	consulError     chan error
	ctx           context.Context
	cancel        context.CancelFunc
	consulCallEnded chan struct{}

	outgoing chan watch.Event
	userStop chan struct{}
	stopped  bool
	stopLock sync.Mutex
				 // wg is used to avoid calls to consul after Stop()
	wg sync.WaitGroup

				 // Injectable for testing. Send the event down the outgoing channel.
	emit func(watch.Event)

	//TODO(mqliang) implement me
	//cache consulCache
}

// watchWaitDuration is the amount of time to wait for an error from watch.
const watchWaitDuration = 100 * time.Millisecond

// newConsulWatcher returns a new consulWatcher; if list is true, watch sub-nodes.  If you provide a transform
// and a versioner, the versioner must be able to handle the objects that transform creates.
func newConsulWatcher(list bool, include includeFunc, filter storage.FilterFunc, encoding runtime.Codec, versioner storage.Versioner, transform TransformFunc/*, cache consulCache*/) *consulWatcher {
	w := &consulWatcher{
		encoding:  encoding,
		versioner: versioner,
		transform: transform,
		list:      list,
		include:   include,
		filter:    filter,
		// Buffer this channel, so that the consul client is not forced
		// to context switch with every object it gets, and so that a
		// long time spent decoding an object won't block the *next*
		// object. Basically, we see a lot of "401 window exceeded"
		// errors from consul, and that's due to the client not streaming
		// results but rather getting them one at a time. So we really
		// want to never block the consul client, if possible. The 100 is
		// mostly arbitrary--we know it goes as high as 50, though.
		// There's a V(2) log message that prints the length so we can
		// monitor how much of this buffer is actually used.
		consulIncoming: make(chan *consulapi.KVPair, 100),
		consulError:    make(chan error, 1),
		outgoing:     make(chan watch.Event),
		userStop:     make(chan struct{}),
		stopped:      false,
		wg:           sync.WaitGroup{},
		//cache:        cache,
		ctx:          nil,
		cancel:       nil,
	}
	w.emit = func(e watch.Event) { w.outgoing <- e }
	go w.translate()
	return w
}

var (
	watchChannelHWM HighWaterMark
)


// translate pulls stuff from consul, converts, and pushes out the outgoing channel. Meant to be
// called as a goroutine.
func (w *consulWatcher) translate() {
	defer close(w.outgoing)
	defer util.HandleCrash()

	for {
		select {
		case err := <-w.consulError:
			if err != nil {
				/*
				var status *unversioned.Status
				switch {
				case consulutil.IsConsulWatchExpired(err):
					status = &unversioned.Status{
						Status:  unversioned.StatusFailure,
						Message: err.Error(),
						Code:    http.StatusGone, // Gone
						Reason:  unversioned.StatusReasonExpired,
					}
				// TODO: need to generate errors using api/errors which has a circular dependency on this package
				//   no other way to inject errors
				// case consulutil.IsConsulUnreachable(err):
				//   status = errors.NewServerTimeout(...)
				default:
				*/
				status := &unversioned.Status{
					Status:  unversioned.StatusFailure,
					Message: err.Error(),
					Code:    http.StatusInternalServerError,
					Reason:  unversioned.StatusReasonInternalError,
				}

			w.emit(watch.Event{
				Type:   watch.Error,
				Object: status,
			})
		}
			return
		case <-w.userStop:
			return
		case res, ok := <-w.consulIncoming:
			if ok {
				if curLen := int64(len(w.consulIncoming)); watchChannelHWM.Update(curLen) {
					// Monitor if this gets backed up, and how much.
					glog.V(2).Infof("watch: %v objects queued in channel.", curLen)
				}
				w.sendResult(res)
			}
		// If !ok, don't return here-- must wait for consulError channel
		// to give an error or be closed.
		}
	}
}

// etcdWatch calls etcd's Watch function, and handles any errors. Meant to be called
// as a goroutine.
func (w *consulWatcher) consulWatch(ctx context.Context, client *consulapi.KV, key string, resourceVersion uint64) {
	defer util.HandleCrash()
	defer close(w.consulError)
	defer close(w.consulIncoming)

	// All calls to etcd are coming from this function - once it is finished
	// no other call to etcd should be generated by this watcher.
	done := func() {}

	// We need to be prepared, that Stop() can be called at any time.
	// It can potentially also be called, even before this function is called.
	// If that is the case, we simply skip all the code here.
	// See #18928 for more details.
	// var watcher etcd.Watcher
	returned := func() bool {
		w.stopLock.Lock()
		defer w.stopLock.Unlock()
		if w.stopped {
			// Watcher has already been stopped - don't event initiate it here.
			return true
		}
		w.wg.Add(1)
		done = w.wg.Done
		// Perform initialization of watcher under lock - we want to avoid situation when
		// Stop() is called in the meantime (which in tests can cause etcd termination and
		// strange behavior here).
		if resourceVersion == 0 {
			latest, err := consulGetInitialWatchState(ctx, client, key, w.list, w.consulIncoming)
			if err != nil {
				w.consulError <- err
				return true
			}
			resourceVersion = latest
		}
		return false
	}()
	defer done()
	if returned {
		return
	}

	if !w.list {
		w.internalWatch(client, key, resourceVersion)
	} else {
		w.internalWatchDir(client, key, resourceVersion)
	}
}

func (w *consulWatcher)internalWatch(client *consulapi.KV, key string, resourceVersion uint64) {
	// Use a wait time in order to check if we should quit
	// from time to time.
	opts := &consulapi.QueryOptions{WaitTime: DefaultWatchWaitTime, WaitIndex: resourceVersion}

	for {
		// Get the key
		pair, meta, err := client.Get(key, opts)
		if err != nil {
			w.consulError <- err
			return
		}

		// If LastIndex didn't change then it means `Get` returned
		// because of the WaitTime and the key didn't changed.
		if opts.WaitIndex == meta.LastIndex {
			continue
		}
		opts.WaitIndex = meta.LastIndex

		// Return the value to the channel
		var response *ConsulResponse
		if pair != nil {
			// Observe a create action
			if pair.CreateIndex == pair.ModifyIndex {
				response.Action = ConsulCreate
				response.Index = meta.LastIndex
				response.Node = pair
			}
			// Observe a modify action
			if pair.CreateIndex != pair.ModifyIndex {
				response.Action = ConsulSet
				response.Index = meta.LastIndex
				response.Node = pair
			}

		} else {
			// Observe a delete action
			response.Action = ConsulDelete
			response.Index = meta.LastIndex
			response.Node = &consulapi.KVPair{}
		}
		w.consulIncoming <- response
	}
}

func (w *consulWatcher)internalWatchDir(client *consulapi.KV, directory string, resourceVersion uint64) {
	// Use a wait time in order to check if we should quit
	// from time to time.
	opts := &consulapi.QueryOptions{WaitTime: DefaultWatchWaitTime, WaitIndex: resourceVersion}
	for {
		// Get all the childrens
		pairs, meta, err := client.List(directory, opts)
		if err != nil {
			return
		}

		// If LastIndex didn't change then it means `Get` returned
		// because of the WaitTime and the child keys didn't change.
		if opts.WaitIndex == meta.LastIndex {
			continue
		}
		opts.WaitIndex = meta.LastIndex

		// Return children KV pairs to the channel
		for _, pair := range pairs {
			if pair.Key == directory {
				continue
			}
			// Return the value to the channel
			var response *ConsulResponse
			if pair != nil {
				// Observe a create action
				if pair.CreateIndex == pair.ModifyIndex {
					response.Action = ConsulCreate
					response.Index = meta.LastIndex
					response.Node = pair
				}
				// Observe a modify action
				if pair.CreateIndex != pair.ModifyIndex {
					response.Action = ConsulSet
					response.Index = meta.LastIndex
					response.Node = pair
				}

			} else {
				// // Observe a delete action
				response.Action = ConsulDelete
				response.Index = meta.LastIndex
				response.Node = &consulapi.KVPair{}
			}
			w.consulIncoming <- response
		}
	}
}

// etcdGetInitialWatchState turns an etcd Get request into a watch equivalent
func consulGetInitialWatchState(ctx context.Context, client *consulapi.KV, key string, recursive bool, incoming chan<- *consulapi.KVPair) (resourceVersion uint64, err error) {
	options := &consulapi.QueryOptions{
		AllowStale:        false,
		RequireConsistent: true,
	}

	pair, meta, err := client.Get(key, options)
	if err != nil {
		return nil, err
	}

	// If pair is nil then the key does not exist
	if pair == nil {
		return nil, fmt.Errorf("KeyNotFound")
	}

	resourceVersion = meta.LastIndex
	//convertRecursiveResponse(resp.Node, resp, incoming)
	return
}

func (w *consulWatcher) sendResult(res *ConsulResponse) {
	switch res.Action {
	case ConsulCreate:
		w.sendAdd(res)
	case ConsulSet:
		w.sendModify(res)
	case ConsulDelete:
		w.sendDelete(res)
	default:
		glog.Errorf("unknown action: %v", res.Action)
	}
}


func (w *consulWatcher) sendAdd(res *ConsulResponse) {
	if res.Node == nil {
		glog.Errorf("unexpected nil node: %#v", res)
		return
	}
	if w.include != nil && !w.include(res.Node.Key) {
		return
	}
	obj, err := w.decodeObject(res.Node)
	if err != nil {
		glog.Errorf("failure to decode api object: '%v' from %#v %#v", string(res.Node.Value), res, res.Node)
		// TODO: expose an error through watch.Interface?
		// Ignore this value. If we stop the watch on a bad value, a client that uses
		// the resourceVersion to resume will never be able to get past a bad value.
		return
	}
	if !w.filter(obj) {
		return
	}
	action := watch.Added
	if res.Node.ModifyIndex != res.Node.CreateIndex {
		action = watch.Modified
	}
	w.emit(watch.Event{
		Type:   action,
		Object: obj,
	})
}

func (w *consulWatcher) sendModify(res *ConsulResponse) {
	if res.Node == nil {
		glog.Errorf("unexpected nil node: %#v", res)
		return
	}
	if w.include != nil && !w.include(res.Node.Key) {
		return
	}
	obj, err := w.decodeObject(res.Node)
	if err != nil {
		glog.Errorf("failure to decode api object: '%v' from %#v %#v", string(res.Node.Value), res, res.Node)
		// TODO: expose an error through watch.Interface?
		// Ignore this value. If we stop the watch on a bad value, a client that uses
		// the resourceVersion to resume will never be able to get past a bad value.
		return
	}
	// Some changes to an object may cause it to start or stop matching a filter.
	// We need to report those as adds/deletes. So we have to check both the previous
	// and current value of the object. Unfortunately, we can not get the previous
	// value of the object since consul doesn't support this, so just check the current
	// value now, it may lead to emit repeated deletion event, but it's no pproblem.
	// TODO(mqliang) there must be more elegant way to implement this(such as save the previous value?).
	action := watch.Modified
	if !w.filter(obj) {
		action = watch.Deleted
	}
	w.emit(watch.Event{
		Type:   action,
		Object: obj,
	})
}

func (w *consulWatcher) sendDelete(res *ConsulResponse) {
	/*if res.PrevNode == nil {
		glog.Errorf("unexpected nil prev node: %#v", res)
		return
	}
	if w.include != nil && !w.include(res.PrevNode.Key) {
		return
	}
	node := *res.PrevNode*/
//	if res.Node != nil {
//		// Note that this sends the *old* object with the etcd index for the time at
//		// which it gets deleted. This will allow users to restart the watch at the right
//		// index.
//		node.ModifiedIndex = res.Node.ModifiedIndex
//	}
	obj, err := w.decodeObject(&node)
	if err != nil {
		glog.Errorf("failure to decode api object: '%v' from %#v %#v", string(res.PrevNode.Value), res, res.PrevNode)
		// TODO: expose an error through watch.Interface?
		// Ignore this value. If we stop the watch on a bad value, a client that uses
		// the resourceVersion to resume will never be able to get past a bad value.
		return
	}
	if !w.filter(obj) {
		return
	}
	w.emit(watch.Event{
		Type:   watch.Deleted,
		Object: obj,
	})
}

func (w *consulWatcher) decodeObject(node *consulapi.KVPair) (runtime.Object, error) {
	/* TODO(mqliang) add cache
	if obj, found := w.cache.getFromCache(node.ModifiedIndex, storage.Everything); found {
		return obj, nil
	}
	*/

	obj, err := w.encoding.Decode(node.Value)
	if err != nil {
		return nil, err
	}

	// ensure resource version is set on the object we load from etcd
	if w.versioner != nil {
		if err := w.versioner.UpdateObject(obj, node.Expiration, node.ModifyIndex); err != nil {
			glog.Errorf("failure to version api object (%d) %#v: %v", node.ModifyIndex, obj, err)
		}
	}

	// perform any necessary transformation
	if w.transform != nil {
		obj, err = w.transform(obj)
		if err != nil {
			glog.Errorf("failure to transform api object %#v: %v", obj, err)
			return nil, err
		}
	}

	if node.ModifiedIndex != 0 {
		w.cache.addToCache(node.ModifiedIndex, obj)
	}
	return obj, nil
}
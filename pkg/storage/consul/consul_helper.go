package consul

import (
	consulapi "github.com/hashicorp/consul/api"
	"k8s.io/kubernetes/pkg/runtime"
	"k8s.io/kubernetes/pkg/storage"
	"net/http"
	"k8s.io/kubernetes/pkg/api"
	"path"
	"golang.org/x/net/context"
	"github.com/golang/glog"
	"strings"
	"errors"
	"time"
	"k8s.io/kubernetes/pkg/conversion"
	"k8s.io/kubernetes/pkg/storage/consul/metrics"
	"reflect"
	"k8s.io/kubernetes/pkg/watch"
)

const (
// RenewSessionRetryMax is the number of time we should try
// to renew the session before giving up and throwing an error
	RenewSessionRetryMax = 5
)

var (
// ErrSessionRenew is thrown when the session can't be
// renewed because the Consul version does not support sessions
	ErrSessionRenew = errors.New("cannot set or renew session for ttl, unable to operate on sessions")
)

var (
// ErrBackendNotSupported is thrown when the backend k/v store is not supported by libkv
	ErrBackendNotSupported = errors.New("Backend storage not supported yet, please choose one of")
// ErrCallNotSupported is thrown when a method is not implemented/supported by the current backend
	ErrCallNotSupported = errors.New("The current call is not supported with this backend")
// ErrNotReachable is thrown when the API cannot be reached for issuing common store operations
	ErrNotReachable = errors.New("Api not reachable")
// ErrCannotLock is thrown when there is an error acquiring a lock on a key
	ErrCannotLock = errors.New("Error acquiring the lock")
// ErrKeyModified is thrown during an atomic operation if the index does not match the one in the store
	ErrKeyModified = errors.New("Unable to complete atomic operation, key modified")
// ErrKeyNotFound is thrown when the key is not found in the store during a Get operation
	ErrKeyNotFound = errors.New("Key not found in store")
// ErrPreviousNotSpecified is thrown when the previous value is not specified for an atomic operation
	ErrPreviousNotSpecified = errors.New("Previous K/V pair should be provided for the Atomic operation")
// ErrKeyExists is thrown when the previous value exists in the case of an AtomicPut
	ErrKeyExists = errors.New("Previous K/V pair exists, cannnot complete Atomic operation")
)


// storage.Config object for consul.
type ConsulConfig struct {
	ServerList []string
	Codec      runtime.Codec
	Prefix     string
}


// implements storage.Config
func (c *ConsulConfig) GetType() string {
	return "consul"
}

// implements storage.Config
func (c *ConsulConfig) NewStorage() (storage.Interface, error) {
//	cfg := etcd.Config{
//		Endpoints: c.ServerList,
//		// TODO: Determine if transport needs optimization
//		Transport: &http.Transport{
//			Proxy: http.ProxyFromEnvironment,
//			Dial: (&net.Dialer{
//				Timeout:   30 * time.Second,
//				KeepAlive: 30 * time.Second,
//			}).Dial,
//			TLSHandshakeTimeout: 10 * time.Second,
//			MaxIdleConnsPerHost: 500,
//		},
//	}
	// Create Consul client
	config := consulapi.DefaultConfig()
	config.HttpClient = http.DefaultClient
	config.Address = c.ServerList[0]
	config.Scheme = "http"
	consulClient, err := consulapi.NewClient(config)
	if err != nil {
		return nil, err
	}
	return NewConsulStorage(consulClient, c.Codec, c.Prefix), nil
}

func NewConsulStorage(client consulapi.Client, codec runtime.Codec, prefix string) storage.Interface {
	return &consulHelper{
		consulclient: client,
		client:     client.KV(),
		codec:      codec,
		//TODO(mqliang):implement this
		versioner:  APIObjectVersioner{},
		copier:     api.Scheme,
		pathPrefix: path.Join("/", prefix),
		//cache:      util.NewCache(maxEtcdCacheEntries),
	}
}


// etcdHelper is the reference implementation of storage.Interface.
type consulHelper struct {
	consulclient consulapi.Client
	client     *consulapi.KV
	codec      runtime.Codec
	copier     runtime.ObjectCopier
	// optional, has to be set to perform any atomic operations
	versioner storage.Versioner
	// prefix for all etcd keys
	pathPrefix string

	// We cache objects stored in etcd. For keys we use Node.ModifiedIndex which is equivalent
	// to resourceVersion.
	// This depends on etcd's indexes being globally unique across all objects/types. This will
	// have to revisited if we decide to do things like multiple etcd clusters, or etcd will
	// support multi-object transaction that will result in many objects with the same index.
	// Number of entries stored in the cache is controlled by maxEtcdCacheEntries constant.
	// TODO: Measure how much this cache helps after the conversion code is optimized.
	// cache util.Cache
}


func init() {
	metrics.Register()
}

// Codec provides access to the underlying codec being used by the implementation.
func (h *consulHelper) Codec() runtime.Codec {
	return h.codec
}

// Implements storage.Interface.
func (h *consulHelper) Backends(ctx context.Context) []string {
	if ctx == nil {
		glog.Errorf("Context is nil")
	}

	members, err := h.consulclient.Agent().Members(true)
	if err != nil {
		glog.Errorf("Error obtaining consul members list: %q", err)
		return nil
	}
	mlist := []string{}
	for _, member := range members {
		mlist = append(mlist, member.Addr+member.Port)
	}
	return mlist
}

// Implements storage.Interface.
func (h *consulHelper) Versioner() storage.Versioner {
	return h.versioner
}

// Implements storage.Interface.
func (h *consulHelper) Create(ctx context.Context, key string, obj, out runtime.Object, ttl uint64) error {
	if ctx == nil {
		glog.Errorf("Context is nil")
	}
	key = h.prefixConsulKey(key)
	data, err := h.codec.Encode(obj)
	if err != nil {
		return err
	}
	if h.versioner != nil {
		if version, err := h.versioner.ObjectResourceVersion(obj); err == nil && version != 0 {
			return errors.New("resourceVersion may not be set on objects to be created")
		}
	}

	startTime := time.Now()

	p := &consulapi.KVPair{
		Key:   key,
		Value: string(data),
	}
	if ttl > 0 {
		// Create or renew a session holding a TTL. Operations on sessions
		// are not deterministic: creating or renewing a session can fail
		for retry := 1; retry <= RenewSessionRetryMax; retry++ {
			err := h.renewSession(p, ttl)
			if err == nil {
				break
			}
			if retry == RenewSessionRetryMax {
				return ErrSessionRenew
			}
		}
	}
	//TODO(mqliang):preVersion
	_, err = h.client.Put(p, nil)
	if err != nil {
		return err
	}

	metrics.RecordEtcdRequestLatency("create", getTypeName(obj), startTime)
	if err != nil {
		return err
	}
	if out != nil {
		if _, err := conversion.EnforcePtr(out); err != nil {
			panic("unable to convert output object to pointer")
		}
		//TODO(mqliang) get and check
	}
	return err
}


// Implements storage.Interface.
func (h *consulHelper) Set(ctx context.Context, key string, obj, out runtime.Object, ttl uint64) error {
	if ctx == nil {
		glog.Errorf("Context is nil")
	}
	//var response *etcd.Response
	data, err := h.codec.Encode(obj)
	if err != nil {
		return err
	}
	key = h.prefixConsulKey(key)

	p := &consulapi.KVPair{
		Key:   key,
		Value: string(data),
	}

	if ttl > 0 {
		// Create or renew a session holding a TTL. Operations on sessions
		// are not deterministic: creating or renewing a session can fail
		for retry := 1; retry <= RenewSessionRetryMax; retry++ {
			err := h.renewSession(p, ttl)
			if err == nil {
				break
			}
			if retry == RenewSessionRetryMax {
				return ErrSessionRenew
			}
		}
	}

	create := true
	if h.versioner != nil {
		if version, err := h.versioner.ObjectResourceVersion(obj); err == nil && version != 0 {
			create = false
			startTime := time.Now()
			//TODO(mqliang):preVersion
			_, err = h.client.Put(p, nil)
			metrics.RecordEtcdRequestLatency("compareAndSwap", getTypeName(obj), startTime)
			if err != nil {
				return err
			}
		}
	}
	if create {
		// Create will fail if a key already exists.
		startTime := time.Now()
		//TODO(mqliang):preVersion
		_, err = h.client.Put(p, nil)
		if err != nil {
			return err
		}
		metrics.RecordEtcdRequestLatency("create", getTypeName(obj), startTime)
	}

	if err != nil {
		return err
	}
	if out != nil {
		if _, err := conversion.EnforcePtr(out); err != nil {
			panic("unable to convert output object to pointer")
		}
		//TODO(mqliang):get and check
	}

	return err
}


// Implements storage.Interface.
func (h *consulHelper) Delete(ctx context.Context, key string, out runtime.Object) error {
	if ctx == nil {
		glog.Errorf("Context is nil")
	}
	key = h.prefixConsulKey(key)
	if _, err := conversion.EnforcePtr(out); err != nil {
		panic("unable to convert output object to pointer")
	}

	startTime := time.Now()
	_, err := h.client.Delete(key, nil)
	metrics.RecordEtcdRequestLatency("delete", getTypeName(out), startTime)
	//TODO(mqliang):implement me
	/*
	if !etcdutil.IsEtcdNotFound(err) {
		// if the object that existed prior to the delete is returned by etcd, update out.
		if err != nil || response.PrevNode != nil {
			_, _, err = h.extractObj(response, err, out, false, true)
		}
	}
	*/
	return err
}

// Implements storage.Interface.
func (h *consulHelper) Watch(ctx context.Context, key string, resourceVersion string, filter storage.FilterFunc) (watch.Interface, error) {
	if ctx == nil {
		glog.Errorf("Context is nil")
	}
	watchRV, err := storage.ParseWatchResourceVersion(resourceVersion)
	if err != nil {
		return nil, err
	}
	key = h.prefixConsulKey(key)
	w := newConsulWatcher(false, nil, filter, h.codec, h.versioner, nil, h)
	go w.consulWatch(ctx, h.client, key, watchRV)
	return w, nil
}







func (h *consulHelper) prefixConsulKey(key string) string {
	if strings.HasPrefix(key, h.pathPrefix) {
		return key
	}
	return path.Join(h.pathPrefix, key)
}


func (s *consulHelper) renewSession(pair *consulapi.KVPair, ttl time.Duration) error {
	// Check if there is any previous session with an active TTL
	session, err := s.getActiveSession(pair.Key)
	if err != nil {
		return err
	}

	if session == "" {
		entry := &consulapi.SessionEntry{
			Behavior:  consulapi.SessionBehaviorDelete, // Delete the key when the session expires
			TTL:       (ttl / 2).String(),        // Consul multiplies the TTL by 2x
			LockDelay: 1 * time.Millisecond,      // Virtually disable lock delay
		}

		// Create the key session
		session, _, err = s.consulclient.Session().Create(entry, nil)
		if err != nil {
			return err
		}

		lockOpts := &consulapi.LockOptions{
			Key:     pair.Key,
			Session: session,
		}

		// Lock and ignore if lock is held
		// It's just a placeholder for the
		// ephemeral behavior
		lock, _ := s.consulclient.LockOpts(lockOpts)
		if lock != nil {
			lock.Lock(nil)
		}
	}

	_, _, err = s.consulclient.Session().Renew(session, nil)
	return err
}

// getActiveSession checks if the key already has
// a session attached
func (s *consulHelper) getActiveSession(key string) (string, error) {
	pair, _, err := s.client.Get(key, nil)
	if err != nil {
		return "", err
	}
	if pair != nil && pair.Session != "" {
		return pair.Session, nil
	}
	return "", nil
}

/*
func (h *consulHelper) extractObj(response *consulapi.KVPair, inErr error, objPtr runtime.Object, ignoreNotFound, prevNode bool) (body string, node *etcd.Node, err error) {
	if response != nil {
		if prevNode {
			node = response.PrevNode
		} else {
			node = response.Node
		}
	}
	if inErr != nil || node == nil || len(node.Value) == 0 {
		if ignoreNotFound {
			v, err := conversion.EnforcePtr(objPtr)
			if err != nil {
				return "", nil, err
			}
			v.Set(reflect.Zero(v.Type()))
			return "", nil, nil
		} else if inErr != nil {
			return "", nil, inErr
		}
		return "", nil, fmt.Errorf("unable to locate a value on the response: %#v", response)
	}
	body = node.Value
	err = h.codec.DecodeInto([]byte(body), objPtr)
	if h.versioner != nil {
		_ = h.versioner.UpdateObject(objPtr, node.Expiration, node.ModifiedIndex)
		// being unable to set the version does not prevent the object from being extracted
	}
	return body, node, err
}
*/

// TODO(mqliang): this function is copy from etcd_gelper.go, remove the duplicating
func getTypeName(obj interface{}) string {
	return reflect.TypeOf(obj).String()
}
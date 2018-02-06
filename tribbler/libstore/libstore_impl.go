package libstore

import (
	"errors"

	"fmt"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"net/rpc"
	"time"
	"sort"
	"sync"
	"github.com/cmu440/tribbler/rpc/librpc"
	"log"
)

type libstore struct {
	store      *rpc.Client
	myHostPort string
	mode       LeaseMode
	hash_ring  []storagerpc.Node
	server     map[uint32]*rpc.Client
	mutex      map[string]*sync.Mutex
	mutex_lock *sync.Mutex
	map_mutex  *sync.Mutex
	epoch_call chan int
	lease_map  map[string]*lease
}

type lease struct {
	times_asked int
	current_second int
	max_second int
	lease  bool
	value  *value
}

type value struct {
	str   string
	str_list []string
}

// NewLibstore creates a new instance of a TribServer's libstore. masterServerHostPort
// is the master storage server's host:port. myHostPort is this Libstore's host:port
// (i.e. the callback address that the storage servers should use to send back
// notifications when leases are revoked).
//
// The mode argument is a debugging flag that determines how the Libstore should
// request/handle leases. If mode is Never, then the Libstore should never request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to false). If mode is Always, then the Libstore should always request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to true). If mode is Normal, then the Libstore should make its own
// decisions on whether or not a lease should be requested from the storage server,
// based on the requirements specified in the project PDF handout.  Note that the
// value of the mode flag may also determine whether or not the Libstore should
// register to receive RPCs from the storage servers.
//
// To register the Libstore to receive RPCs from the storage servers, the following
// line of code should suffice:
//
//     rpc.RegisterName("LeaseCallbacks", librpc.Wrap(libstore))
//
// Note that unlike in the NewTribServer and NewStorageServer functions, there is no
// need to create a brand new HTTP handler to serve the requests (the Libstore may
// simply reuse the TribServer's HTTP handler since the two run in the same process).
func NewLibstore(masterServerHostPort, myHostPort string, mode LeaseMode) (Libstore, error) {
	master, err := rpc.DialHTTP("tcp", masterServerHostPort)
	if err != nil {
		fmt.Println("connecting storage error")
		return nil, err
	}
	// connect to get hash ring
	args := &storagerpc.GetServersArgs{}
	reply := &storagerpc.GetServersReply{}
	_ = master.Call("StorageServer.GetServers", args, reply)
	var count int;
	for count = 0; count < 5; count++  {
		time.Sleep(time.Second * 1)
		_ = master.Call("StorageServer.GetServers", args, reply)
		if reply.Status == storagerpc.OK {
			break
		}
	}
	// try up to 5 times and quit
	if count >= 5 {
		return nil, nil
	}
	// get hash ring.
	lib := &libstore{
		master,
		myHostPort,
		mode,
		reply.Servers,
		make(map[uint32]*rpc.Client),
		make(map[string]*sync.Mutex),
		new(sync.Mutex),
		new(sync.Mutex),
		make(chan int, 1),
		make(map[string]*lease),

	}
	lib.sortHashRing()
	//log.Print(lib.hash_ring)
	// register revoke list callback
	rpc.RegisterName("LeaseCallbacks", librpc.Wrap(lib))
	go lib.epoch()
	return lib, nil
}

func (ls *libstore) Get(key string) (string, error) {
	ls.map_mutex.Lock()
	defer ls.map_mutex.Unlock()
	// start new lease entry
	if _, ok := ls.lease_map[key]; !ok {
		ls.lease_map[key] = &lease{
			1,
			0,
			0,
			false,
			&value{},
		}
		// use old entry
	} else {
		if ls.lease_map[key].lease {
			return ls.lease_map[key].value.str, nil
		} else {
			ls.lease_map[key].times_asked++
		}
	}
	// request a lease or not
	var tmp bool
	if ls.lease_map[key].times_asked > storagerpc.QueryCacheThresh {
		tmp = true
	} else {
		tmp = false
	}

	// for lease mode
	if ls.mode == LeaseMode(Always) {
		tmp = true
	} else if ls.mode == LeaseMode(Never) {
		tmp = false
	}

	args := &storagerpc.GetArgs{
		key,
		tmp,
		ls.myHostPort,
	}
	reply := &storagerpc.GetReply{}
	// call different storage
	conn := ls.getConnections(StoreHash(key))
	err := conn.Call("StorageServer.Get", args, reply)
//	for err != nil {
//		err = conn.Call("StorageServer.Get", args, reply)
//	}
	if err != nil {
		return "", errors.New("rpc call fail")
	}
	if reply.Status == storagerpc.OK {
		if reply.Lease.Granted {
			ls.lease_map[key].value.str = reply.Value
			ls.lease_map[key].current_second = 0
			ls.lease_map[key].max_second = reply.Lease.ValidSeconds
			ls.lease_map[key].lease = true
		}
		return reply.Value, nil
	}
	return "", errors.New("reply is not ok")
}

func (ls *libstore) Put(key, value string) error {
	args := &storagerpc.PutArgs{
		key,
		value,
	}
	reply := &storagerpc.PutReply{}
	// call different storage
	conn := ls.getConnections(StoreHash(key))
	err := conn.Call("StorageServer.Put", args, reply)
//	for err != nil {
//		err = conn.Call("StorageServer.Put", args, reply)
//	}
	if err != nil {
		return errors.New("rpc call fail")
	}
	if reply.Status == storagerpc.OK {
		return nil
	}
	return errors.New("reply is not ok")

}

func (ls *libstore) Delete(key string) error {
	args := &storagerpc.DeleteArgs{
		key,
	}
	reply := &storagerpc.DeleteReply{}
	// call different storage
	conn := ls.getConnections(StoreHash(key))
	err := conn.Call("StorageServer.Delete", args, reply)
//	for err != nil {
//		err = conn.Call("StorageServer.Delete", args, reply)
//	}
	if err != nil {
		return errors.New("rpc call fail")
	}
	if reply.Status == storagerpc.OK {
		return nil
	}
	return errors.New("reply is not ok")
}

func (ls *libstore) GetList(key string) ([]string, error) {
	ls.map_mutex.Lock()
	defer ls.map_mutex.Unlock()

	if _, ok := ls.lease_map[key]; !ok {
		ls.lease_map[key] = &lease{
			1,
			0,
			0,
			false,
			&value{},
		}
		// use old entry
	} else {
		if ls.lease_map[key].lease {
			return ls.lease_map[key].value.str_list, nil
		} else {
			ls.lease_map[key].times_asked++
		}
	}
	// request a lease or not
	var tmp bool
	if ls.lease_map[key].times_asked > storagerpc.QueryCacheThresh {
		tmp = true
	} else {
		tmp = false
	}

	if ls.mode == LeaseMode(Always) {
		tmp = true
	} else if ls.mode == LeaseMode(Never) {
		tmp = false
	}
	args := &storagerpc.GetArgs{
		key,
		tmp,
		ls.myHostPort,
	}
	reply := &storagerpc.GetListReply{}
	// call different storage
	conn := ls.getConnections(StoreHash(key))
	err := conn.Call("StorageServer.GetList", args, reply)
//	for err != nil {
//		err = conn.Call("StorageServer.GetList", args, reply)
//	}
	if err != nil {
		return nil, errors.New("rpc call fail")
	}
	if reply.Status == storagerpc.OK {
		if reply.Lease.Granted {
			ls.lease_map[key].value.str_list = reply.Value
			ls.lease_map[key].current_second = 0
			ls.lease_map[key].max_second = reply.Lease.ValidSeconds
			ls.lease_map[key].lease = true
		}
		return reply.Value, nil
	}
	return nil, errors.New("reply is not ok")
}

func (ls *libstore) RemoveFromList(key, removeItem string) error {
	args := &storagerpc.PutArgs{
		key,
		removeItem,
	}
	reply := &storagerpc.PutReply{}
	// call different storage
	conn := ls.getConnections(StoreHash(key))
	err := conn.Call("StorageServer.RemoveFromList", args, reply)
//	for err != nil {
//		err = conn.Call("StorageServer.RemoveFromList", args, reply)
//	}
	if err != nil {
		return errors.New("rpc call fail")
	}
	if reply.Status == storagerpc.OK {
		return nil
	}
	return errors.New("reply is not ok")
}

func (ls *libstore) AppendToList(key, newItem string) error {
	args := &storagerpc.PutArgs{
		key,
		newItem,
	}
	reply := &storagerpc.PutReply{}
	conn := ls.getConnections(StoreHash(key))
	err := conn.Call("StorageServer.AppendToList", args, reply)
//	for err != nil {
//		err = conn.Call("StorageServer.AppendToList", args, reply)
//	}
	if err != nil {
		return errors.New("rpc call fail")
	}
	if reply.Status == storagerpc.OK {
		return nil
	}
	return errors.New("reply is not ok")
}
// no error will occur, only key ot found
func (ls *libstore) RevokeLease(args *storagerpc.RevokeLeaseArgs, reply *storagerpc.RevokeLeaseReply) error {
	ls.map_mutex.Lock()
	defer ls.map_mutex.Unlock()
	for k := range ls.lease_map {
		if k != args.Key {
			continue
		}
		delete(ls.lease_map, k)
		reply.Status = storagerpc.OK
		return nil
	}
	reply.Status = storagerpc.KeyNotFound
	return nil

}

type ById []storagerpc.Node

func (a ById) Len() int {
	return len(a)
}

func (a ById) Less(i, j int) bool {
	if a[j].NodeID > a[i].NodeID {
		return true
	} else {
		return false
	}
}

func (a ById) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func (ls *libstore)sortHashRing() {
	sort.Sort(ById(ls.hash_ring))
}

func (ls *libstore) getConnections(hash uint32) *rpc.Client {
	last_item := ls.hash_ring[len(ls.hash_ring) - 1]
	if hash > last_item.NodeID {
		return ls.getOneConnection(0)
	} else {
		for i := 0; i < len(ls.hash_ring); i++ {
			if hash <= ls.hash_ring[i].NodeID {
				return ls.getOneConnection(i)
			} else {
				continue
			}
		}
	}
	fmt.Println("should not be here")
	return nil
}

func (ls *libstore) getOneConnection(index int) *rpc.Client {
	item := ls.hash_ring[index]
	if _, ok := ls.server[item.NodeID]; !ok {
		conn, err := rpc.DialHTTP("tcp", item.HostPort)
		if err != nil {
			log.Println("cannot connect to storage slave, should not happen")
		}
		ls.server[item.NodeID] = conn
		return conn
	} else {
		return ls.server[item.NodeID]
	}
}

func (ls *libstore) getMutex(key string) {
	ls.mutex_lock.Lock()
	defer ls.mutex_lock.Unlock()
	if _, ok := ls.mutex[key]; ok {
		ls.mutex[key].Lock()
	} else {
		ls.mutex[key] = &sync.Mutex{}
		ls.mutex[key].Lock()
	}
}

// suppose only caller will call this
func (ls *libstore) releaseMutex(key string) {
	ls.mutex[key].Unlock()
}

func (ls *libstore) epoch() {
	for {
		time.Sleep(time.Second * 1)
		ls.clean_cache()
	}
}
// need mutex
func (ls * libstore) clean_cache() {
		ls.map_mutex.Lock()
		defer ls.map_mutex.Unlock()
		for k := range ls.lease_map {
			ls.lease_map[k].current_second++
			if ls.lease_map[k].lease == false {
				if ls.lease_map[k].current_second > storagerpc.QueryCacheSeconds {
					delete(ls.lease_map, k)
				}
			} else {
				if ls.lease_map[k].current_second > ls.lease_map[k].max_second {
					delete(ls.lease_map, k)
				}
			}
		}
}
package storageserver

import (
	// "errors"
	"net"
	"net/rpc"
	"net/http"
	"fmt"
	"time"
	"sync"
	"strconv"

	"github.com/cmu440/tribbler/rpc/storagerpc"
	"github.com/cmu440/tribbler/libstore"
)
 
type keyData struct {
	keyMutex		sync.Mutex
	data 			interface{}		// any type
	grantedLease	map[string]*leaseInfo	// store granted leases, hostport-->lease time info
}

type leaseInfo struct {
	startTime		time.Time		// time when granting lease
}

type storageServer struct {
	// TODO: implement this!
	isMaster			bool
	masterHostPort		string
	numNodes			int
	port 				int
	nodeID				uint32
	serverMutex			sync.Mutex
	ring				[]storagerpc.Node
	ringMap				map[uint32]storagerpc.Node
	dataMutex			sync.Mutex
	keyMap				map[string](*keyData)
	clientMap			map[string]*rpc.Client 		// map the hostPort to Client which is used when making RPC calls
	clientMutex			sync.Mutex
}

// NewStorageServer creates and starts a new StorageServer. masterServerHostPort
// is the master storage server's host:port address. If empty, then this server
// is the master; otherwise, this server is a slave. numNodes is the total number of
// servers in the ring. port is the port number that this server should listen on.
// nodeID is a random, unsigned 32-bit ID identifying this server.
//
// This function should return only once all storage servers have joined the ring,
// and should return a non-nil error if the storage server could not be started.
func NewStorageServer(masterServerHostPort string, numNodes, port int, nodeID uint32) (StorageServer, error) {
	var master bool
	if len(masterServerHostPort) == 0 {
		master = true
	} else {
		master = false
	}
	
	serverNode := storagerpc.Node {
		HostPort:		":" + strconv.Itoa(port),
		NodeID:			nodeID,
	}
	
	ss := storageServer {
		isMaster:			master,
		masterHostPort:		masterServerHostPort,
		numNodes:			numNodes,
		port:				port,
		nodeID:				nodeID,
		ring:				make([]storagerpc.Node, 0),
		ringMap:			make(map[uint32]storagerpc.Node),
		serverMutex:		*new(sync.Mutex),
		keyMap:				make(map[string]*keyData),
		dataMutex:			*new(sync.Mutex),
		clientMap:			make(map[string]*rpc.Client),
		clientMutex:		*new(sync.Mutex),
	}
	
	if ss.isMaster {
		// liseten for incoming connections
		listener, err := net.Listen("tcp", ":" + strconv.Itoa(port))
		if err != nil {
			fmt.Printf("Master storage server error: listen for incoming connections %s.\n", err)
			return nil, err
		}
		// regisster itself to receive RPCs from the other servers
		err = rpc.RegisterName("StorageServer", storagerpc.Wrap(&ss))
		err = rpc.RegisterName("MasterStorageServer", storagerpc.Wrap(&ss))
		if err != nil {
			fmt.Printf("NewStorageServer:: register error %s.\n", err)
			return nil, err
		}
		rpc.HandleHTTP()
		go http.Serve(listener, nil)
		// add master server itself to the ring slice and ring map
		registerArgs := &storagerpc.RegisterArgs {
			ServerInfo:		serverNode,
		}
		registerReply := &storagerpc.RegisterReply{}
		ss.RegisterServer(registerArgs, registerReply)
	} else {
		// slave server
		listener, err := net.Listen("tcp", ":" + strconv.Itoa(port))
		if err != nil {
			fmt.Printf("NewStorageServer:: slave storage server error: %s.\n", err)
			return nil, err
		}
		err = rpc.RegisterName("StorageServer", storagerpc.Wrap(&ss))
		if err != nil {
			fmt.Printf("NewStorageServer:: slave storage server error: %s.\n", err)
			return nil, err
		}
		rpc.HandleHTTP()
		go http.Serve(listener, nil)
		// slave storage server dial http to connect to the master storage server
		var client *rpc.Client
		client, err = rpc.DialHTTP("tcp", masterServerHostPort)
		for err != nil {
			fmt.Printf("NewStorageServer:: slave storage server: %d DialHTTP failed.\n", nodeID)
			client, err = rpc.DialHTTP("tcp", masterServerHostPort)
		}
		// register slave server itself as part of the consistent hashing ring
		var registerArgs storagerpc.RegisterArgs
		registerArgs.ServerInfo = serverNode
		registerReply := &storagerpc.RegisterReply{}
		for {
			err = client.Call("MasterStorageServer.RegisterServer", registerArgs, registerReply)
			if registerReply.Status == storagerpc.OK {
				// fmt.Printf("NewStorageServer:: slave storage server: %d registered successfully.\n", nodeID)
				break
			}
			if registerReply.Status == storagerpc.NotReady {
				// fmt.Printf("NewStorageServer:: slave storage server: %d waits for all servers registered.\n", nodeID)
				time.Sleep(time.Second)
//				err = client.Call("MasterStorageServer.RegisterServer", registerArgs, registerReply)
			}
		}
		ss.ring = registerReply.Servers
	}
	if ss.isMaster {
		if len(ss.ring) < ss.numNodes {
			time.Sleep(time.Second)
		}
		// fmt.Println("NewStorageServer:: all servers added")
	}
	return &ss, nil
}

// RegisterServer adds a storage server to the ring. It replies with
// status NotReady if not all nodes in the ring have joined. Once
// all nodes have joined, it should reply with status OK and a list
// of all connected nodes in the ring.
func (ss *storageServer) RegisterServer(args *storagerpc.RegisterArgs, reply *storagerpc.RegisterReply) error {
	ss.serverMutex.Lock()
	serverNode := args.ServerInfo
	// check whether the slave storage server has been added to the ring already
	_, ok := ss.ringMap[serverNode.NodeID]
	if !ok {
		ss.ringMap[serverNode.NodeID] = serverNode
		ss.ring = append(ss.ring, serverNode)
	}
	ss.serverMutex.Unlock()
	if len(ss.ring) < ss.numNodes {
		// fmt.Printf("RegisterServer:: node Id: %d, not ready.\n", serverNode.NodeID)
		reply.Status = storagerpc.NotReady
	} else if len(ss.ring) == ss.numNodes {
		reply.Status = storagerpc.OK
		reply.Servers = ss.ring
		 fmt.Printf("RegisterServer:: node Id: %d, Ok.\n", serverNode.NodeID)
	}
	return nil
}

// Partition returns the first node in the ring after or equal to the key's hash value.
// Given the key, use libstore/StoreHash function to hash the key.
func (ss *storageServer) Partition(key string) storagerpc.Node {
	keyHash := libstore.StoreHash(key)
	
	targetNode := ss.ring[0]
	minNode := ss.ring[0]
	for _, node := range ss.ring {
		// equal to the key's hash value, immediately return
		if node.NodeID == keyHash {
			return node
		}
		// the node is after the key's hash value, and smaller than the current found first node
		// update the first node
		if node.NodeID > keyHash && (targetNode.NodeID < keyHash || node.NodeID < targetNode.NodeID) {
			targetNode = node
		}
		if node.NodeID < minNode.NodeID {
			minNode = node
		}
	}
	// key's hash value is greater than the maximum node, then its successor should be the minimum node
	if targetNode.NodeID >= keyHash {
//		fmt.Printf("Partition:: target node: %d.\n", targetNode.NodeID)
		return targetNode
	} else {
//		fmt.Printf("Partition:: minimum node: %d.\n", minNode.NodeID)
		return minNode
	}
}

// GetServers retrieves a list of all connected nodes in the ring. It
// replies with status NotReady if not all nodes in the ring have joined.
// Request from Libstore
func (ss *storageServer) GetServers(args *storagerpc.GetServersArgs, reply *storagerpc.GetServersReply) error {
	if ss.numNodes > len(ss.ring) {
		// fmt.Printf("GetServers:: not ready. Current: %d, total: %d.\n", len(ss.ring), ss.numNodes)
		reply.Status = storagerpc.NotReady
	} else if len(ss.ring) == ss.numNodes {
		reply.Status = storagerpc.OK
		reply.Servers = ss.ring
		fmt.Printf("GetServers:: completed.\n")
	}
	return nil
}

func (ss *storageServer) GrantLease(args *storagerpc.GetArgs, value *keyData) {
	grantedLeases := value.grantedLease

	// there is no granted lease, add it into the list
	if grantedLeases == nil {
		grantedLeases = make(map[string]*leaseInfo)
		leaseInfo := &leaseInfo {	
			startTime:		time.Now(),
		}
		grantedLeases[args.HostPort] = leaseInfo
	} else {
		existedLease, ok := grantedLeases[args.HostPort]
		if ok {
			// lease already existed. Update the time
			existedLease.startTime = time.Now()
		} else {
			// create a new lease
			existedLease = &leaseInfo {
				startTime:			time.Now(),
			}
		}
		grantedLeases[args.HostPort] = existedLease
		}
	// update the keyMap
	ss.dataMutex.Lock()
	ss.keyMap[args.Key].grantedLease = grantedLeases
	ss.dataMutex.Unlock()
}

// Get retrieves the specified key from the data store and replies with
// the key's value and a lease if one was requested. If the key does not
// fall within the storage server's range, it should reply with status
// WrongServer. If the key is not found, it should reply with status
// KeyNotFound.
func (ss *storageServer) Get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {
	rightNode := ss.Partition(args.Key).NodeID
	if rightNode != ss.nodeID {
		reply.Status = storagerpc.WrongServer
		fmt.Printf("Get:: get key: %s went to the wrong server: %d.\n", args.Key, ss.nodeID)
		return nil
	}
	var value *keyData
	var ok bool
	ss.dataMutex.Lock()
	value, ok = ss.keyMap[args.Key]
	ss.dataMutex.Unlock()
	
	// key is found 
	if ok {
		value.keyMutex.Lock()
		reply.Status = storagerpc.OK
		reply.Value = value.data.(string)
		if args.WantLease {
			ss.GrantLease(args, value)
			var lease storagerpc.Lease
			lease.Granted = true
			lease.ValidSeconds = storagerpc.LeaseSeconds
			reply.Lease = lease
		}
		value.keyMutex.Unlock()
		fmt.Printf("Get:: key %s completed.\n", args.Key)
	} else {
		// Key is not found
		reply.Status = storagerpc.KeyNotFound
		fmt.Printf("Get:: key %s is not found.\n", args.Key)
	}
	return nil
}

func (ss *storageServer) RevokeLease(value *keyData, key string) {
	value.keyMutex.Lock()
	grantedLeases := value.grantedLease
	for hostPort, lease := range grantedLeases {
		replyChan := make(chan int)
		// if the lease is not expired
		if time.Now().Before(lease.startTime.Add(time.Duration(storagerpc.LeaseSeconds + storagerpc.LeaseGuardSeconds) * time.Second)) {
			ss.clientMutex.Lock()
			client, ok := ss.clientMap[hostPort]
			var err error
			if !ok {
				client, err = rpc.DialHTTP("tcp", hostPort)
				if err != nil {
					ss.clientMutex.Unlock()
					// fmt.Printf("RevokeLease:: dial http error: %s.\n", err)
					continue
				}
				ss.clientMap[hostPort] = client
			}
			ss.clientMutex.Unlock()
			revokeLeaseArgs := &storagerpc.RevokeLeaseArgs {
				Key:		key,
			}
			revokeLeaseReply := &storagerpc.RevokeLeaseReply{}
			go ss.ClientRevokeLease(client, revokeLeaseArgs, revokeLeaseReply, replyChan)
			go ss.LeaseExpire(replyChan, lease)
			
			<- replyChan
		}
	}
	value.grantedLease = make(map[string]*leaseInfo)
	value.keyMutex.Unlock()
	ss.dataMutex.Lock()
	ss.keyMap[key] = value
	ss.dataMutex.Unlock()
}

func (ss *storageServer) LeaseExpire(replyChan chan int, lease *leaseInfo) {
	duration := lease.startTime.Add(time.Duration(storagerpc.LeaseSeconds + storagerpc.LeaseGuardSeconds) * time.Second).Sub(time.Now())
	tick := time.Tick(duration)
	
	for {
		select {
			case <- tick:
				replyChan <- 1
				return
		}
	}
}

func (ss *storageServer) ClientRevokeLease(client *rpc.Client, args *storagerpc.RevokeLeaseArgs, 
	reply *storagerpc.RevokeLeaseReply, replyChan chan int) error {
	err := client.Call("LeaseCallbacks.RevokeLease", args, reply)
	if err != nil {
		replyChan <- -1
		return err
	}
	if reply.Status == storagerpc.OK {
		// fmt.Printf("ClientRevokeLease:: revoke lease completed.\n")
		replyChan <- 1
	} else {
		replyChan <- -1
	}
	return nil
}

// Delete remove the specified key from the data store.
// If the key does not fall within the storage server's range,
// it should reply with status WrongServer.
// If the key is not found, it should reply with status KeyNotFound.
func (ss *storageServer) Delete(args *storagerpc.DeleteArgs, reply *storagerpc.DeleteReply) error {
	if ss.Partition(args.Key).NodeID != ss.nodeID {
		reply.Status = storagerpc.WrongServer
		fmt.Printf("Delete:: key: %s wrong server.\n", args.Key)
		return nil
	}
	
	var ok bool
	var value *keyData
	ss.dataMutex.Lock()
	value, ok = ss.keyMap[args.Key]
	ss.dataMutex.Unlock()
	
	if ok {
		ss.RevokeLease(value, args.Key)
		ss.dataMutex.Lock()
		delete(ss.keyMap, args.Key)
		reply.Status = storagerpc.OK
		fmt.Printf("Delete key %s completed.\n", args.Key)
		ss.dataMutex.Unlock()
	} else {
		reply.Status = storagerpc.KeyNotFound
		fmt.Printf("Delete:: key %s is not found.\n", args.Key)
	}
	return nil
}

// GetList retrieves the specified key from the data store and replies with
// the key's list value and a lease if one was requested. If the key does not
// fall within the storage server's range, it should reply with status
// WrongServer. If the key is not found, it should reply with status
// KeyNotFound.
func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	fmt.Printf("GetList:: key: %s.\n", args.Key)
	if ss.Partition(args.Key).NodeID != ss.nodeID {
		reply.Status = storagerpc.WrongServer
		fmt.Printf("GetList:: key: %s wrong server.\n", args.Key)
		return nil
	}
	
	var value *keyData
	var ok bool
	ss.dataMutex.Lock()
	value, ok = ss.keyMap[args.Key]
	ss.dataMutex.Unlock()
	
	if ok {
		value.keyMutex.Lock()
		defer value.keyMutex.Unlock()
		reply.Status = storagerpc.OK
		reply.Value = value.data.([]string)
		if args.WantLease {
			ss.GrantLease(args, value)
			
			var lease storagerpc.Lease
			lease.Granted = true
			lease.ValidSeconds = storagerpc.LeaseSeconds
			reply.Lease = lease
		}
		fmt.Printf("GetList:: get key %s completed.\n", args.Key)
	} else {
		reply.Status = storagerpc.KeyNotFound
		fmt.Printf("GetList:: key %s is not found.\n", args.Key)
	}
	
	return nil
}

// Put inserts the specified key/value pair into the data store. If
// the key does not fall within the storage server's range, it should
// reply with status WrongServer.
func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	fmt.Printf("Put:: key: %s, value: %s.\n", args.Key, args.Value)
	if ss.Partition(args.Key).NodeID != ss.nodeID {
		reply.Status = storagerpc.WrongServer
		// fmt.Printf("Put:: key: %s wrong server.\n", args.Key)
		return nil
	}
	
	var value *keyData
	var ok bool
	ss.dataMutex.Lock()
	value, ok = ss.keyMap[args.Key]
	// The key does not exist(no granted lease). Insert the key/value pair.
	if !ok {
		keyValue := &keyData {
			keyMutex: 			*new(sync.Mutex),
			data:				args.Value,
			
		}
		ss.keyMap[args.Key] = keyValue
		reply.Status = storagerpc.OK
		fmt.Printf("Put:: insert key %s and value %s completed.\n", args.Key, args.Value)
		ss.dataMutex.Unlock()
	} else {
		// The key exists, check whether there are granted leases. If there are, revoke them, then update the key/value pair
		ss.dataMutex.Unlock()
		ss.RevokeLease(value, args.Key)
		
		reply.Status = storagerpc.OK
		value.keyMutex.Lock()
		ss.keyMap[args.Key].data = args.Value
		value.keyMutex.Unlock()
		fmt.Printf("Put:: update key %s and value %s completed.\n", args.Key, args.Value)
	}
	return nil
}

// AppendToList retrieves the specified key from the data store and appends
// the specified value to its list. If the key does not fall within the
// receiving server's range, it should reply with status WrongServer. If
// the specified value is already contained in the list, it should reply
// with status ItemExists.
func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	fmt.Printf("AppendToList:: key: %s, value: %s.\n", args.Key, args.Value)
	if ss.Partition(args.Key).NodeID != ss.nodeID {
		reply.Status = storagerpc.WrongServer
		fmt.Printf("AppendToList:: key: %s wrong server.\n", args.Key)
		return nil
	}

	var value *keyData
	var ok bool
	ss.dataMutex.Lock()
	value, ok = ss.keyMap[args.Key]

	if ok {
		ss.dataMutex.Unlock()
		ss.RevokeLease(value, args.Key)
		value.keyMutex.Lock()
		values := value.data.([]string)
		for _, v := range values {
			if v == args.Value {
				reply.Status = storagerpc.ItemExists
				fmt.Printf("AppendToList:: value %s already exists.\n", args.Value)
				value.keyMutex.Unlock()
				return nil
			}
		}
		values = append(values, args.Value)
		ss.dataMutex.Lock()
		ss.keyMap[args.Key].data = values
		ss.dataMutex.Unlock()
		reply.Status = storagerpc.OK
		fmt.Printf("AppendToList:: append value %s to key %s completed.\n", args.Value, args.Key)
		value.keyMutex.Unlock()
	} else {
		values := make([]string, 0)
		values = append(values, args.Value)
		keyValue := &keyData{
			keyMutex: 		*new(sync.Mutex),
			data:			values,
		}
		ss.keyMap[args.Key] = keyValue
		ss.dataMutex.Unlock()
		reply.Status = storagerpc.OK
		 fmt.Printf("AppendToList:: insert key %s and value %s completed.\n", args.Key, args.Value)
	}
	return nil
}

// RemoveFromList retrieves the specified key from the data store and removes
// the specified value from its list. If the key does not fall within the
// receiving server's range, it should reply with status WrongServer. If
// the specified value is not already contained in the list, it should reply
// with status ItemNotFound.
func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	fmt.Printf("DeleteFromList:: key: %s, value: %s.\n", args.Key, args.Value)
	if ss.Partition(args.Key).NodeID != ss.nodeID {
		reply.Status = storagerpc.WrongServer
		fmt.Printf("RemoveFromList:: key: %s wrong server.\n", args.Key)	
		return nil
	}
	var value *keyData
	var ok bool
	ss.dataMutex.Lock()
	value, ok = ss.keyMap[args.Key]
	
	// The key does not exist
	if ok {
		ss.dataMutex.Unlock()
		ss.RevokeLease(value, args.Key)
		value.keyMutex.Lock()
		values := value.data.([]string)
		for i, v := range values {
			if v == args.Value {
				values = append(values[:i], values[i+1:]...)
				ss.dataMutex.Lock()
				ss.keyMap[args.Key].data = values
				ss.dataMutex.Unlock()
				reply.Status = storagerpc.OK
				fmt.Printf("RemoveFromList:: remove value %s from key %s list completed.\n", args.Value, args.Key)
				value.keyMutex.Unlock()
				return nil
			}
		}
		reply.Status = storagerpc.ItemNotFound
		fmt.Printf("RemoveFromList:: value %s not found.\n", args.Value)
		value.keyMutex.Unlock()
	} else {
		reply.Status = storagerpc.KeyNotFound
		ss.dataMutex.Unlock()
		fmt.Printf("RemoveFromList:: key: %s not found.\n", args.Key)
	}
	
	return nil
}

package shardkv

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"slices"
	"time"

	"umich.edu/eecs491/proj4/common"
)

//
// Define what goes into "value" that Paxos is used to agree upon.
// Field names must start with capital letters
//
type Op struct {
	Shard 	 	int
	ConfigNum	int
	NewKVStore	map[string]string 	// New key-value store to apply for shard assignment
	NewOPCache	map[int64]int64		// New operation cache to apply for shard assignment

	Key      	string
	Value    	string
	Op       	string
	ClientID 	int64
	OpID     	int64
}

type Get struct {
	args 	GetArgs
	reply 	*GetReply
	done 	chan error
}

type PutAppend struct {
	args	PutAppendArgs
	reply  	*PutAppendReply
	done 	chan error
}

type Assign struct {
	args	common.AssignArgs
	reply 	*common.AssignReply
	done	chan error
}

type Pull struct {
	args 	PullArgs
	reply 	*PullReply
	done 	chan error
}

//
// Method used by PaxosRSM to determine if two Op values are identical
//
func equals(v1 interface{}, v2 interface{}) bool {
	switch v1Typed := v1.(type) {
	case []byte:
		if v2Typed, ok := v2.([]byte); ok {
			return bytes.Equal(v1Typed, v2Typed)
		}
	}
	return v1 == v2
}

//
// additions to ShardKV state
//
type ShardKVImpl struct {
	shardKVMap	map[int](map[string]string)		// Mapping of shards to their key-value stores
	shardList 	[]int							// List of shards owned by this server
	OPCache		map[int64]int64					// Cache of the latest operation ID per client
	configNum 	int								// Current configuration number


	getChan		chan Get
	paChan		chan PutAppend
	assignChan 	chan Assign
	pullChan	chan Pull
	end			chan interface{}
}

//
// initialize kv.impl.*
//
func (kv *ShardKV) InitImpl() {
	kv.impl.shardKVMap = make(map[int]map[string]string)
	kv.impl.shardList = make([]int, 0)
	kv.impl.OPCache = make(map[int64]int64)
	kv.impl.configNum = 0

	kv.impl.getChan = make(chan Get)
	kv.impl.paChan = make(chan PutAppend)
	kv.impl.assignChan = make(chan Assign)
	kv.impl.pullChan = make(chan Pull)
	kv.impl.end = make(chan interface{})

	// Register Op for encoding with Gob
	gob.Register(Op{})
	
	// Goroutine to close the `end` channel when the server is dead
	go func() {
		defer close(kv.impl.end)
		for !kv.isdead() { time.Sleep(1000) }
	}()
	
	// Goroutine to handle incoming requests
	go kv.requestHandler()
}

func (kv *ShardKV) requestHandler() {
	for {
		select {
		case req := <-kv.impl.getChan:
			// Handle GET Request
			req.done <- kv.handleGet(&req.args, req.reply)

		case req := <-kv.impl.paChan:
			// Handle PutAppend Request
			req.done <- kv.handlePutAppend(&req.args, req.reply)

		case req := <-kv.impl.assignChan:
			// Handle Assign Request
			req.done <- kv.handleAssignShard(&req.args)

		case req := <-kv.impl.pullChan:
			// Handle Pull Request
			req.done <- kv.handlePullShard(&req.args, req.reply)

		case <-kv.impl.end:
			// Terminate program
			return
		}
	}
}

//
// RPC handler for client Get requests
//
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	req := Get{
		args: *args,
		reply: reply,
		done: make(chan error),
	}

	select {
	case kv.impl.getChan <- req:
		// Wait for handleGet to complete
		return <-req.done
	case <-kv.impl.end:
		// Prevent deadlock
		return nil
	}
}

func (kv *ShardKV) handleGet(args *GetArgs, reply *GetReply) error {
	// Initialize client operation ID in cache if not present
	_, exists := kv.impl.OPCache[args.ClientID]
	if !exists {
		kv.impl.OPCache[args.ClientID] = -1
	}

	// Create and encode the Get operation
	req := Op{
		Key: args.Key,
		Op: "Get",
		ClientID: args.ClientID,
		OpID: args.OpID,
	}
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	if err := encoder.Encode(req); err != nil {
		fmt.Println("Error encoding operation:", err)
		return err
	}

	// Add the operation to the Paxos sequence
	kv.rsm.AddOp(buf.Bytes())

	// Retrieve the value from the shard's key-value store
	shard := common.Key2Shard(args.Key)
	if !slices.Contains(kv.impl.shardList, shard) {
		reply.Err = ErrWrongGroup	// Shard doesn't belong to this server
	} else if _, exists := kv.impl.shardKVMap[shard][args.Key]; !exists {
		reply.Err = ErrNoKey		// Key not found in the shard
	} else {
		reply.Err = OK				// Successful retrieval
		reply.Value = kv.impl.shardKVMap[shard][args.Key]
	}

	return nil
}


//
// RPC handler for client Put and Append requests
//
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	req := PutAppend{
		args: *args,
		reply: reply,
		done: make(chan error),
	}

	select {
	case kv.impl.paChan <- req:
		// Wait for handlePutAppend to complete
		return <-req.done
	case <-kv.impl.end:
		// Prevent deadlock
		return nil
	}
}

func (kv *ShardKV) handlePutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Initialize client operation ID in cache if not present
	_, exists := kv.impl.OPCache[args.ClientID]
	if !exists {
		kv.impl.OPCache[args.ClientID] = -1
	}

	// Skip processing if the request is already handled
	if args.OpID <= kv.impl.OPCache[args.ClientID] {
		reply.Err = OK
		return nil
	}

	// Create and encode the PutAppend operation
	req := Op{
		Key: args.Key,
		Value: args.Value,
		Op: args.Op,
		ClientID: args.ClientID,
		OpID: args.OpID,
	}
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	if err := encoder.Encode(req); err != nil {
		fmt.Println("Error encoding operation:", err)
		return err
	}

	// Add the operation to the Paxos sequence
	kv.rsm.AddOp(buf.Bytes())

	// Check if the shard this key belongs to is owned by this server
	shard := common.Key2Shard(args.Key)
	if !slices.Contains(kv.impl.shardList, shard) {
		reply.Err = ErrWrongGroup		// Server doesn't own the shard for this key
	} else {
		reply.Err = OK					// Operation was successful
	}

	return nil
}


//
// Assign a shard to this group. Called by ShardMaster
//
func (kv *ShardKV) AssignShard(args *common.AssignArgs, reply *common.AssignReply) error {
	req := Assign{
		args: *args,
		reply: reply,
		done: make(chan error),
	}

	select {
	case kv.impl.assignChan <- req:
		// Wait for handleAssignShard to complete
		return <-req.done
	case <-kv.impl.end:
		// Prevent deadlock
		return nil
	}
}

func (kv *ShardKV) handleAssignShard(args *common.AssignArgs) error {
	// Check if the config number is outdated
	if args.ConfigNum < kv.impl.configNum {
		return nil
	}

	// Initialize the Assign operation
	req := Op{
		Op:        "Assign",
		Shard:     args.Shard,
		ConfigNum: args.ConfigNum,
		NewKVStore: make(map[string]string),
		NewOPCache: make(map[int64]int64),
	}

	// Set up arguments for pulling the shard data from another server
	arg := PullArgs{
		ConfigNum: args.ConfigNum,
		Shard:     args.Shard,
	}

	// Try to pull the shard data from available servers
	pulled := false
	for !pulled {
		if len(args.Servers) == 0 {
			break
		}

		for i := 0; i < len(args.Servers); i++ {
			var rep PullReply
			// Call PullShard RPC to retrieve the shard data
			if common.Call(args.Servers[i], "ShardKV.PullShard", arg, &rep) {
				if rep.Err == OK {
					req.NewKVStore = rep.KVStore
					req.NewOPCache = rep.OpIDCache
					pulled = true
					break
				} else if rep.Err == ErrOld {
					return nil
				}
			}
		}
	}

	// Encode the Assign operation
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	if err := encoder.Encode(req); err != nil {
		fmt.Println("Error encoding operation:", err)
		return err
	}

	// Add the operation to the Paxos sequence
	kv.rsm.AddOp(buf.Bytes())

	return nil
}

//
// Pull a shard from this group. Called by another shardkv server
//
func (kv *ShardKV) PullShard(args *PullArgs, reply *PullReply) error {
	req := Pull{
		args: *args,
		reply: reply,
		done: make(chan error),
	}

	select {
	case kv.impl.pullChan <- req:
		// Wait for handlepullShard to complete
		return <-req.done
	case <-kv.impl.end:
		// Prevent deadlock
		return nil
	}
}

func (kv *ShardKV) handlePullShard(args *PullArgs, reply *PullReply) error {
	// Check if the config number is outdated
	if args.ConfigNum < kv.impl.configNum {
		reply.Err = ErrOld
		return nil
	}
	
	// Create and encode the Pull operation
	req := Op{
		Op: "Pull",
		ConfigNum: args.ConfigNum,
		Shard: args.Shard,
	}
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	if err := encoder.Encode(req); err != nil {
		fmt.Println("Error encoding operation:", err)
		return err
	}

	// Add the operation to the Paxos sequence
	kv.rsm.AddOp(buf.Bytes())

	// If the config number matches, send the shard data; otherwise, return ErrOld
	if args.ConfigNum == kv.impl.configNum {
		reply.Err = OK
		reply.KVStore = kv.impl.shardKVMap[args.Shard]
		reply.OpIDCache = kv.impl.OPCache
	} else {
		reply.Err = ErrOld
	}

	return nil
}

//
// Execute operation encoded in decided value v and update local state
//
func (kv *ShardKV) ApplyOp(v interface{}) {
	// Decode the operation
	data, _ := v.([]byte)
    var op Op
    decoder := gob.NewDecoder(bytes.NewReader(data))
    if err := decoder.Decode(&op); err != nil {
        fmt.Println("AddOp: failed to decode Op:", err)
        return
    }

	switch op.Op {
		
	// "Get": Update OPCache, no state change
	case "Get":
		shard := common.Key2Shard(op.Key)
		if slices.Contains(kv.impl.shardList, shard) {
			kv.impl.OPCache[op.ClientID] = max(op.OpID, kv.impl.OPCache[op.ClientID])
		}

	// "Put": Add key-value pair to shard's KV store
	case "Put":
		shard := common.Key2Shard(op.Key)
		if slices.Contains(kv.impl.shardList, shard) { // Check if this shard belongs to the server
			// Ensure the shard's KV map exists
			if kv.impl.shardKVMap[shard] == nil {
				kv.impl.shardKVMap[shard] = make(map[string]string) // Initialize the map if it doesn't exist
			}
			kv.impl.shardKVMap[shard][op.Key] = op.Value // Store the new value
			kv.impl.OPCache[op.ClientID] = op.OpID		 // Update OPCache
		}

	// "Append": Append value to existing key
	case "Append":
		shard := common.Key2Shard(op.Key)
		if slices.Contains(kv.impl.shardList, shard) { // Check if this shard belongs to the server
			// Ensure the shard's KV map exists
			if kv.impl.shardKVMap[shard] == nil {
				kv.impl.shardKVMap[shard] = make(map[string]string) // Initialize the map if it doesn't exist
			}
			kv.impl.shardKVMap[shard][op.Key] += op.Value // Append the value
			kv.impl.OPCache[op.ClientID] = op.OpID		  // Update OPCache
		}

	// "Assign": Assign new shard, merge OPCache
	case "Assign":
		// Ignore outdated configuration numbers
		if op.ConfigNum < kv.impl.configNum {
			return
		}

		// Check if the shard already exists in shardList
		for _, shard := range kv.impl.shardList {
			if shard == op.Shard {
				return
			}
		}

		// Merge OPCache with the new OP cache, taking the maximum values for each client
		updatedOPCache := make(map[int64]int64)
		for client, value := range kv.impl.OPCache {
			if newValue, exists := op.NewOPCache[client]; exists {
				updatedOPCache[client] = max(value, newValue)
			} else {
				updatedOPCache[client] = value
			}
		}

		// Add any new clients from the new OP cache
		for client, newValue := range op.NewOPCache {
			if _, exists := kv.impl.OPCache[client]; !exists {
				updatedOPCache[client] = newValue
			}
		}

		// Update server state with the new shard and configuration
		kv.impl.shardList = append(kv.impl.shardList, op.Shard)
		kv.impl.shardKVMap[op.Shard] = op.NewKVStore
		kv.impl.configNum = op.ConfigNum
		kv.impl.OPCache = updatedOPCache
		
		return

	// "Pull": Remove a shard from the server's state
	case "Pull":
		// Ignore outdated configuration numbers
		if op.ConfigNum < kv.impl.configNum {
			return
		}

		// Find the index of the shard to remove
		index := -1
		for i := 0; i < len(kv.impl.shardList); i++ {
			if kv.impl.shardList[i] == op.Shard {
				index = i
				break
			}
		}

		// Remove the shard from the shard list
		if index != -1 {
			kv.impl.shardList = append(kv.impl.shardList[:index], kv.impl.shardList[index+1:]...)
		}

		// Update to the new configuration number
		kv.impl.configNum = op.ConfigNum

		return
	}
}
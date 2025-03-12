package shardmaster

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"sort"
	"time"

	"umich.edu/eecs491/proj4/common"
)

//
// Define what goes into "value" that Paxos is used to agree upon.
// Field names must start with capital letters.
//
type Op struct {
	ReqNum 		int
	Op			string
	GID			int64
	Shard 		int
	Servers 	[]string
	Num 		int
	Me 			int
}

type ShardMovement struct {
	ShardID     int
	ConfigNum	int
	OldServers  []string
	NewServers  []string
}

type Join struct {
	GID     int64
	Servers []string
	done	chan interface{}
}

type Leave struct {
	GID 	int64
	done	chan interface{}
}

type Move struct {
	Shard 	int
	GID   	int64
	done	chan interface{}
}

type Query struct {
	Num	 	int
	config	chan Config
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
// additions to ShardMaster state
//
type ShardMasterImpl struct {
	configStore		[]Config
	ShardMovements 	[]ShardMovement
	requestNum		int

	joinChan		chan Join
	leaveChan		chan Leave
	moveChan		chan Move
	queryChan		chan Query
	end				chan interface{}
}

//
// initialize sm.impl.*
//
func (sm *ShardMaster) InitImpl() {
	// Initialize configStore with a default configuration
	sm.impl.configStore = make([]Config, 0)
	config := Config{
		Num:    0,
		Shards: [common.NShards]int64{},
		Groups: make(map[int64][]string),
	}
	sm.impl.configStore = append(sm.impl.configStore, config)
	sm.impl.requestNum = 0


	sm.impl.joinChan = make(chan Join)
	sm.impl.leaveChan = make(chan Leave)
	sm.impl.moveChan = make(chan Move)
	sm.impl.queryChan = make(chan Query)
	sm.impl.end = make(chan interface{})

	// Register Op for encoding with Gob
	gob.Register(Op{})
	
	// Goroutine to close the `end` channel when the server is dead
	go func() {
		defer close(sm.impl.end)
		for !sm.isdead() { time.Sleep(1000) }
	}()

	// Start the main request handler
	go sm.requestHandler()
}

func (sm *ShardMaster) requestHandler() {
	for {
		select {
		case req := <-sm.impl.joinChan:
			// Handle JOIN Request
			sm.handleJoin(req.GID, req.Servers)
			close(req.done)

		case req := <-sm.impl.leaveChan:
			// Handle Leave request
			sm.handleLeave(req.GID)
			close(req.done)

		case req := <-sm.impl.moveChan:
			// Handle Move request
			sm.handleMove(req.Shard, req.GID)
			close(req.done)

		case req := <-sm.impl.queryChan:
			// Handle Query request
			req.config <- sm.handleQuery(req.Num)

		case <-sm.impl.end:
			// Terminate program
			return
		}
	}
}

//
// RPC handlers for Join RPC
//
func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	req := Join{
		GID: args.GID,
		Servers: args.Servers,
		done: make(chan interface{}),
	}

	select {
	case sm.impl.joinChan <- req:
		<-req.done
	case <-sm.impl.end:
		// Prevent deadlock
	}
	return nil
}

func (sm *ShardMaster) handleJoin(GID int64, servers []string) error {
	// Encode the "Join" operation
	req := Op{
		ReqNum: sm.impl.requestNum,
		Op: "Join",
		GID: GID,
		Servers: servers,
		Me: sm.me,
	}
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	if err := encoder.Encode(req); err != nil {
		fmt.Println("Error encoding operation:", err)
		return err
	}

	// Add the operation to the Paxos sequence
	sm.rsm.AddOp(buf.Bytes())

	// Notify servers and clear the map
	sm.notifyServers()
	sm.impl.ShardMovements = sm.impl.ShardMovements[:0]

	return nil
}

//
// RPC handlers for Leave RPC
//
func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	req := Leave{
		GID: args.GID,
		done: make(chan interface{}),
	}

	select {
	case sm.impl.leaveChan <- req:
		<-req.done
	case <-sm.impl.end:
		// Prevent deadlock
	}
	return nil
}

func (sm *ShardMaster) handleLeave(GID int64) error {
	// Encode the "Leave" operation
	req := Op{
		ReqNum: sm.impl.requestNum,
		Op: "Leave",
		GID: GID,
		Me: sm.me,
	}
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	if err := encoder.Encode(req); err != nil {
		fmt.Println("Error encoding operation:", err)
		return err
	}

	// Add the operation to the Paxos sequence
	sm.rsm.AddOp(buf.Bytes())

	// Notify servers and clear the shard movements
	sm.notifyServers()
	sm.impl.ShardMovements = sm.impl.ShardMovements[:0]
	return nil
}

//
// RPC handlers for Move RPC
//
func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	req := Move{
		Shard: args.Shard,
		GID: args.GID,
		done: make(chan interface{}),
	}
	
	select {
	case sm.impl.moveChan <- req:
		<-req.done
	case <-sm.impl.end:
		// Prevent deadlock
	}

	return nil
}

func (sm *ShardMaster) handleMove(shard int, GID int64) error {
	// Create and encode the Move operation
	req := Op{
		ReqNum: sm.impl.requestNum,
		Op: "Move",
		Shard: shard,
		GID: GID,
		Me: sm.me,
	}
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	if err := encoder.Encode(req); err != nil {
		fmt.Println("Error encoding operation:", err)
		return err
	}

	// Add Move operation to Paxos sequence
	sm.rsm.AddOp(buf.Bytes())

	// Notify servers and clear the map
	sm.notifyServers()
	sm.impl.ShardMovements = sm.impl.ShardMovements[:0]

	return nil
}

//
// RPC handlers for Query RPC
//
func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	req := Query{
		Num: args.Num,
		config: make(chan Config),
	}

	select {
	case sm.impl.queryChan <- req:
		reply.Config = <-req.config
	case <-sm.impl.end:
		// Prevent deadlock
	}

	return nil
}

func (sm *ShardMaster) handleQuery(num int) Config {
	// Create and encode Query Operation
	req := Op{
		ReqNum: sm.impl.requestNum,
		Op: "Query",
		Num: num,
		Me: sm.me,
	}
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	if err := encoder.Encode(req); err != nil {
		fmt.Println("Error encoding operation:", err)
		return Config{}
	}

	// Determine which configuration to return
	if num == -1 || num >= len(sm.impl.configStore) {
		// Add Query operation to Paxos sequence
		sm.rsm.AddOp(buf.Bytes())

		// Return the latest configuration
		return sm.impl.configStore[len(sm.impl.configStore)-1]
	}

	// Return the requested configuration
	return sm.impl.configStore[num]
}

//
// Execute operation encoded in decided value v and update local state
//
func (sm *ShardMaster) ApplyOp(v interface{}) {
	// Decode the operation
	data, _ := v.([]byte)
    var op Op
    decoder := gob.NewDecoder(bytes.NewReader(data))
    if err := decoder.Decode(&op); err != nil {
        fmt.Println("AddOp: failed to decode Op:", err)
        return
    }

	// Retrieve the latest config and create new one
	currentConfig := sm.impl.configStore[len(sm.impl.configStore) - 1]
	newConfig := Config{
		Num:    len(sm.impl.configStore),
		Shards: currentConfig.Shards,
		Groups: make(map[int64][]string),
	}
	for gid, servers := range currentConfig.Groups {
		newConfig.Groups[gid] = servers
	}

	// Apply the operation based on its type
	switch op.Op {
	case "Join":
		// Add the new group to the configuration
		newConfig.Groups[op.GID] = op.Servers
		// Redistribute the shards
		sm.rebalanceShards(&newConfig, make([]string, 0))
		
	case "Leave":
		// Remove the leaving group from the configuration
		prevServers := newConfig.Groups[op.GID]
		delete(newConfig.Groups, op.GID)

		// Mark shards belonging to the leaving group as unassigned
		for shard, gid := range newConfig.Shards {
			if gid == op.GID {
				newConfig.Shards[shard] = 0 // Mark as unassigned
			}
		}

		// Redistribute unassigned shards among the remaining groups
		sm.rebalanceShards(&newConfig, prevServers)

	case "Move":
		// Directly assign the shard to the specified group
		prevGID := currentConfig.Shards[op.Shard]
		if prevGID != op.GID {
			// Track shard movement
			sm.impl.ShardMovements = append(sm.impl.ShardMovements, ShardMovement{
				ShardID:    op.Shard,
				ConfigNum: 	newConfig.Num,		
				OldServers: currentConfig.Groups[prevGID],
				NewServers: currentConfig.Groups[op.GID],
			})

			newConfig.Shards[op.Shard] = op.GID
		}
	}

	sm.impl.requestNum++
	sm.impl.configStore = append(sm.impl.configStore, newConfig)
}

func (sm *ShardMaster) rebalanceShards(config *Config, prevServers []string) {
	// Calculate the target number of shards per group
	numGroups := len(config.Groups)
	targetShardsPerGroup := 16 / numGroups
	extraShards := 16 % numGroups

	// Count current shards per group
    shardsPerGroup := make(map[int64]int)
    for _, gid := range config.Shards {
        if gid != 0 { // Ignore unassigned shards
            shardsPerGroup[gid]++
        }
    }

    // Create a sorted list of available group IDs
    availableGroups := make([]int64, 0, len(config.Groups))
    for gid := range config.Groups {
		availableGroups = append(availableGroups, gid)
    }
    sort.Slice(availableGroups, func(i, j int) bool {
        return availableGroups[i] < availableGroups[j]
    })

    // Identify groups with overloaded and underloaded shards
    overloadedGroups := make(map[int64]int)
    underloadedGroups := make(map[int64]int)
    for _, gid := range availableGroups {
        target := targetShardsPerGroup
        current := shardsPerGroup[gid]

        if extraShards > 0 {
            target++
            extraShards--
        }

        if current > target {
            overloadedGroups[gid] = current - target
        } else if current < target {
            underloadedGroups[gid] = target - current
        }
    }

    // Redistribute unassigned shards first
    for i := 0; i < common.NShards; i++ {
        if config.Shards[i] == 0 { // Unassigned shard
            for _, gid := range availableGroups {
                if underloadedGroups[gid] > 0 {
                    config.Shards[i] = gid
                    underloadedGroups[gid]--
                    shardsPerGroup[gid]++
					
					if len(prevServers) != 0 {
						// Track shard movement
						sm.impl.ShardMovements = append(sm.impl.ShardMovements, ShardMovement{
							ShardID:    i,
							ConfigNum: 	config.Num,
							OldServers: prevServers,
							NewServers: config.Groups[gid],
						})
					} else {
						// Track shard movement
						sm.impl.ShardMovements = append(sm.impl.ShardMovements, ShardMovement{
							ShardID:    i,
							ConfigNum: 	config.Num,
							OldServers: make([]string, 0),
							NewServers: config.Groups[gid],
						})
					}
                    break
                }
            }
        }
    }

    // Redistribute from overloaded groups
    for i := 0; i < common.NShards; i++ {
        currentGID := config.Shards[i]

        // Skip if this group isn't overloaded or shard is unassigned
        if currentGID == 0 || overloadedGroups[currentGID] <= 0 {
            continue
        }

        // Find an underloaded group to move this shard to
        for _, gid := range availableGroups {
            if underloadedGroups[gid] > 0 {
                config.Shards[i] = gid
                overloadedGroups[currentGID]--
                underloadedGroups[gid]--
                shardsPerGroup[currentGID]--
                shardsPerGroup[gid]++

				// Track shard movement
				sm.impl.ShardMovements = append(sm.impl.ShardMovements, ShardMovement{
					ShardID:    i,
					ConfigNum: 	config.Num,
					OldServers: config.Groups[currentGID],
					NewServers: config.Groups[gid],
				})
                break
            }
        }
    }
}

func (sm *ShardMaster) notifyServers() {
	for _, movement := range sm.impl.ShardMovements {
		// Extract information from the shard movement
		configNum := movement.ConfigNum
		shard := movement.ShardID
		oldGroups := movement.OldServers
		newGroups := movement.NewServers

		// Notify each server in the new group of the shard assignment
		arg := common.AssignArgs{
			ConfigNum: configNum,
			Shard:     shard,
			Servers:   oldGroups,
		}

		// Try to assign shard to one of the servers in the new group
		assigned := false
		for !assigned {
			for _, server := range newGroups {
				var rep common.AssignReply
				ok := common.Call(server, "ShardKV.AssignShard", arg, &rep)
				if ok {
					assigned = true
					break
				}
			}
		}
	}
}
package shardkv

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"umich.edu/eecs491/proj4/common"
	"umich.edu/eecs491/proj4/shardmaster"
)

// information about the servers of one replica group.
type tGroup struct {
	gid     int64
	servers []*ShardKV
	ports   []string
}

// information about all the servers of a k/v cluster.
type tCluster struct {
	t           *testing.T
	masters     []*shardmaster.ShardMaster
	mck         *shardmaster.Clerk
	masterports []string
	groups      []*tGroup
}

func port(tag string, host int) string {
	s := "/var/tmp/824-"
	s += strconv.Itoa(os.Getuid()) + "/"
	os.Mkdir(s, 0777)
	s += "skv-"
	s += strconv.Itoa(os.Getpid()) + "-"
	s += tag + "-"
	s += strconv.Itoa(host)
	return s
}

//
// start a k/v replica server thread.
//
func (tc *tCluster) start1(gi int, si int, unreliable bool) {
	s := StartServer(tc.groups[gi].gid, tc.groups[gi].ports, si)
	tc.groups[gi].servers[si] = s
	s.Setunreliable(unreliable)
}

func (tc *tCluster) cleanup() {
	for gi := 0; gi < len(tc.groups); gi++ {
		g := tc.groups[gi]
		for si := 0; si < len(g.servers); si++ {
			if g.servers[si] != nil {
				g.servers[si].kill()
			}
		}
	}

	for i := 0; i < len(tc.masters); i++ {
		if tc.masters[i] != nil {
			tc.masters[i].Kill()
		}
	}
}

func (tc *tCluster) shardclerk() *shardmaster.Clerk {
	return shardmaster.MakeClerk(tc.masterports)
}

func (tc *tCluster) clerk() *Clerk {
	return MakeClerk(tc.masterports)
}

func (tc *tCluster) join(gi int) {
	tc.mck.Join(tc.groups[gi].gid, tc.groups[gi].ports)
}

func (tc *tCluster) leave(gi int) {
	tc.mck.Leave(tc.groups[gi].gid)
}

func setup(t *testing.T, tag string, unreliable bool) *tCluster {
	runtime.GOMAXPROCS(4)

	const nmasters = 3
	const ngroups = 3   // replica groups
	const nreplicas = 3 // servers per group

	tc := &tCluster{}
	tc.t = t
	tc.masters = make([]*shardmaster.ShardMaster, nmasters)
	tc.masterports = make([]string, nmasters)

	for i := 0; i < nmasters; i++ {
		tc.masterports[i] = port(tag+"m", i)
	}
	for i := 0; i < nmasters; i++ {
		tc.masters[i] = shardmaster.StartServer(tc.masterports, i)
	}
	tc.mck = tc.shardclerk()

	tc.groups = make([]*tGroup, ngroups)

	for i := 0; i < ngroups; i++ {
		tc.groups[i] = &tGroup{}
		tc.groups[i].gid = int64(i + 100)
		tc.groups[i].servers = make([]*ShardKV, nreplicas)
		tc.groups[i].ports = make([]string, nreplicas)
		for j := 0; j < nreplicas; j++ {
			tc.groups[i].ports[j] = port(tag+"s", (i*nreplicas)+j)
		}
		for j := 0; j < nreplicas; j++ {
			tc.start1(i, j, unreliable)
		}
	}

	// return smh, gids, ha, sa, clean
	return tc
}

func TestBasic(t *testing.T) {
	tc := setup(t, "basic", false)
	defer tc.cleanup()

	fmt.Printf("Test: Basic Join/Leave ...\n")

	tc.join(0)

	ck := tc.clerk()

	ck.Put("a", "x")
	ck.Append("a", "b")
	if ck.Get("a") != "xb" {
		t.Fatalf("Get got wrong value")
	}

	rr := rand.New(rand.NewSource(int64(os.Getpid())))
	keys := make([]string, 10)
	vals := make([]string, len(keys))
	for i := 0; i < len(keys); i++ {
		keys[i] = strconv.Itoa(rr.Int())
		vals[i] = strconv.Itoa(rr.Int())
		ck.Put(keys[i], vals[i])
	}

	// are keys still there after joins?
	for g := 1; g < len(tc.groups); g++ {
		tc.join(g)
		for i := 0; i < len(keys); i++ {
			v := ck.Get(keys[i])
			if v != vals[i] {
				t.Fatalf("joining; wrong value; g=%v k=%v wanted=%v got=%v",
					g, keys[i], vals[i], v)
			}
			vals[i] = strconv.Itoa(rr.Int())
			ck.Put(keys[i], vals[i])
		}
	}

	// are keys still there after leaves?
	for g := 0; g < len(tc.groups)-1; g++ {
		tc.leave(g)
		for i := 0; i < len(keys); i++ {
			v := ck.Get(keys[i])
			if v != vals[i] {
				t.Fatalf("leaving; wrong value; g=%v k=%v wanted=%v got=%v",
					g, keys[i], vals[i], v)
			}
			vals[i] = strconv.Itoa(rr.Int())
			ck.Put(keys[i], vals[i])
		}
	}

	fmt.Printf("  ... Passed\n")
}

func TestLinearizability(t *testing.T) {
    tc := setup(t, "linearizability", true)
    defer tc.cleanup()

    fmt.Printf("Test: Linearizability under concurrent operations ...\n")

    tc.join(0)
    tc.join(1)
    tc.join(2)

    ck := tc.clerk()

    // Key for testing
    key := "x"

    // Initial value
    ck.Put(key, "0")

    // Perform concurrent operations
    const numOperations = 100
    const numClients = 5
    var wg sync.WaitGroup
    var counter int32 = 0 // Use int32 for atomic operations
    results := make([]string, numOperations)

    for clientID := 0; clientID < numClients; clientID++ {
        wg.Add(1)
        go func(clientID int) {
            defer wg.Done()
            for i := 0; i < numOperations/numClients; i++ {
                op := rand.Intn(2) // 0 for Put, 1 for Get
                if op == 0 {
                    // newValue := fmt.Sprintf("%d-%d", clientID, i)
					log.Printf("Client%d appending 0 in loop%d", clientID, i)
                    ck.Append(key, "0")
					log.Printf("Client%d appended 0 in loop%d", clientID, i)
                } else {
					log.Printf("Client%d Getting key in loop %d", clientID, i)
                    v := ck.Get(key)
                    idx := atomic.AddInt32(&counter, 1) - 1 // Safely increment counter
					log.Printf("Client%d Got key in loop %d. Stored in index%d", clientID, i, idx)
                    results[idx] = v
                }
            }
        }(clientID)
    }

    // Simulate shard movement during the operations
	shard := common.Key2Shard(key)
    go func() {
        for i := 0; i < 1000; i++ {
            // Move shard 0 between groups
            if i%3 == 0 {
				log.Print("Shard moving to gid100")
                ck.sm.Move(shard, 100) // Assign to group 100
				log.Print("Shard moved to gid100")
            } else if i%3 == 1{
				log.Print("Shard moving to gid101")
                ck.sm.Move(shard, 101) // Assign to group 101
				log.Print("Shard moved to gid101")
            } else {
				log.Print("Shard moving to gid102")
				ck.sm.Move(shard, 102) // Assign to group 102
				log.Print("Shard moved to gid102")
			}

			time.Sleep(50)
        }
    }()

    wg.Wait()

	for i, value := range results {
		// Check if this is not the first element
		if i > 0 {
			// Compare the digit count of the previous value and current value
			if len(results[i-1]) > len(value) {
				t.Fatalf("Error: Value at index %d (%s) has more digits than value at index %d (%s)", i-1, results[i-1], i, value)
			}
		}
		log.Printf("Index %d = %s\n", i, value)
	}

    fmt.Printf("  ... Passed\n")
}

func TestLinearizabilityMultiKey(t *testing.T) {
    tc := setup(t, "linearizability-multikey", true)
    defer tc.cleanup()

    fmt.Printf("Test: Linearizability with multiple keys and shard group dynamics ...\n")

    // Add all shard groups
	for i := 0; i < len(tc.groups); i++ {
		tc.join(i)
	}

	log.Print("Gets here0")

    ck := tc.clerk()

    // Number of keys and clients
    const numKeys = 3
    const numClients = 3

    // Channels to store key/value pairs
    type KeyValue struct {
        Key   string
        Value string
    }
    keyValueChans := make([]chan KeyValue, numKeys)
    for i := 0; i < numKeys; i++ {
		log.Printf("Index = %d", i)
        keyValueChans[i] = make(chan KeyValue, 1)
        key := fmt.Sprintf("key%d", i)
        value := fmt.Sprintf("value%d", rand.Intn(1000))
        keyValueChans[i] <- KeyValue{Key: key, Value: value}

        // Put initial key/value pairs into the system
        ck.Put(key, value)
    }

	log.Print("Gets here1")

    // Verify initial key/values are correct
    for _, ch := range keyValueChans {
        kv := <-ch
        receivedValue := ck.Get(kv.Key)
        if receivedValue != kv.Value {
            t.Fatalf("Initial value mismatch for key %s: expected %s, got %s", kv.Key, kv.Value, receivedValue)
        }
        ch <- kv
    }

	log.Print("Gets here2")

    // Start concurrent client goroutines
    var wg sync.WaitGroup

    for clientID := 0; clientID < numClients; clientID++ {
        wg.Add(1)
        go func(clientID int) {
            defer wg.Done()
            rand.Seed(time.Now().UnixNano() + int64(clientID))

            for iter := 0; iter < 50; iter++ {
                idx := rand.Intn(numKeys)
                ch := keyValueChans[idx]

                // Read key/value pair from the channel
                kv := <-ch

                // Verify the current value
                currentValue := ck.Get(kv.Key)
                if currentValue != kv.Value {
                    t.Fatalf("Client %d: value mismatch for key %s: expected %s, got %s", clientID, kv.Key, kv.Value, currentValue)
                }

                // Decide to either Put or Append
                newValue := kv.Value
                if rand.Intn(2) == 0 {
                    newValue = fmt.Sprintf("%s-put", kv.Value)
                    ck.Put(kv.Key, newValue)
                } else {
                    ck.Append(kv.Key, "-append")
                    newValue += "-append"
                }

                // Update the key/value pair and write it back to the channel
                ch <- KeyValue{Key: kv.Key, Value: newValue}
            }
        }(clientID)
    }

    // Start shard group dynamic goroutine
    wg.Add(1)
    go func() {
        defer wg.Done()
        aliveGroups := make(map[int]bool)
		for i := 0; i < len(tc.groups); i++ {
			aliveGroups[i] = true
		}

        for iter := 0; iter < 20; iter++ {
            group := rand.Intn(len(tc.groups))
            if aliveGroups[group] {
                if len(aliveGroups) > 1 {
					log.Printf("Leaving.... Group = %d, aliveness = %t, aliveGroups = %v", group, aliveGroups[group], aliveGroups)
                    tc.leave(group)
                    delete(aliveGroups, group)
                }
            } else {
				log.Printf("Joining.... Group = %d, aliveness = %t, aliveGroups = %v", group, aliveGroups[group], aliveGroups)
                tc.join(group)
                aliveGroups[group] = true
            }

            time.Sleep(time.Second * 2)
        }
    }()

    wg.Wait()

    fmt.Printf("  ... Passed\n")
}



func TestMove(t *testing.T) {
	tc := setup(t, "move", false)
	defer tc.cleanup()

	fmt.Printf("Test: Shards really move ...\n")

	tc.join(0)

	ck := tc.clerk()

	const NKeys = 20
	keys := make([]string, NKeys)
	vals := make([]string, len(keys))
	rr := rand.New(rand.NewSource(int64(os.Getpid())))
	for i := 0; i < len(keys); i++ {
		keys[i] = strconv.Itoa(rr.Int())
		vals[i] = strconv.Itoa(rr.Int())
		ck.Put(keys[i], vals[i])
	}

	// add group 1.
	tc.join(1)

	// check that keys are still there.
	for i := 0; i < len(keys); i++ {
		if ck.Get(keys[i]) != vals[i] {
			t.Fatalf("missing key/value")
		}
	}

	// remove sockets from group 0.
	for _, port := range tc.groups[0].ports {
		os.Remove(port)
	}

	count := int32(0)
	var mu sync.Mutex
	for i := 0; i < len(keys); i++ {
		go func(me int) {
			myck := tc.clerk()
			v := myck.Get(keys[me])
			if v == vals[me] {
				mu.Lock()
				atomic.AddInt32(&count, 1)
				mu.Unlock()
			} else {
				t.Fatalf("Get(%v) yielded %v\n", me, v)
			}
		}(i)
	}

	time.Sleep(10 * time.Second)

	ccc := int(atomic.LoadInt32(&count))

	sc := tc.shardclerk()
	config := sc.Query(-1)
	expected := 0
	for i := 0; i < len(keys); i++ {
		shard := common.Key2Shard(keys[i])
		if tc.groups[1].gid == config.Shards[shard] {
			expected++
		}
	}

	if ccc == expected {
		//fmt.Printf("success = %v, expected = %v\n", ccc, expected)
		fmt.Printf("\n--- \"Shards really move\" passed ---\n")
	} else {
		t.Fatalf("%v keys worked after killing 1/2 of groups; wanted %v",
			ccc, expected)
	}
}

func TestLimp(t *testing.T) {
	tc := setup(t, "limp", false)
	defer tc.cleanup()

	fmt.Printf("Test: Reconfiguration with some dead replicas ...\n")

	tc.join(0)

	ck := tc.clerk()
	rr := rand.New(rand.NewSource(int64(os.Getpid())))

	ck.Put("a", "b")
	if ck.Get("a") != "b" {
		t.Fatalf("got wrong value")
	}

	// kill one server from each replica group.
	for gi := 0; gi < len(tc.groups); gi++ {
		sa := tc.groups[gi].servers
		ns := len(sa)
		sa[rr.Int()%ns].kill()
	}

	keys := make([]string, 20)
	vals := make([]string, len(keys))
	for i := 0; i < len(keys); i++ {
		keys[i] = strconv.Itoa(rr.Int())
		vals[i] = strconv.Itoa(rr.Int())
		ck.Put(keys[i], vals[i])
	}

	// are keys still there after joins?
	for g := 1; g < len(tc.groups); g++ {
		tc.join(g)
		for i := 0; i < len(keys); i++ {
			v := ck.Get(keys[i])
			if v != vals[i] {
				t.Fatalf("joining; wrong value; g=%v k=%v wanted=%v got=%v",
					g, keys[i], vals[i], v)
			}
			vals[i] = strconv.Itoa(rr.Int())
			ck.Put(keys[i], vals[i])
		}
	}

	// are keys still there after leaves?
	for gi := 0; gi < len(tc.groups)-1; gi++ {
		tc.leave(gi)
		g := tc.groups[gi]
		for i := 0; i < len(g.servers); i++ {
			g.servers[i].kill()
		}
		for i := 0; i < len(keys); i++ {
			v := ck.Get(keys[i])
			if v != vals[i] {
				t.Fatalf("leaving; wrong value; g=%v k=%v wanted=%v got=%v",
					g, keys[i], vals[i], v)
			}
			vals[i] = strconv.Itoa(rr.Int())
			ck.Put(keys[i], vals[i])
		}
	}

	fmt.Printf("  ... Passed\n")
}

func doConcurrent(t *testing.T, unreliable bool) {
	tc := setup(t, "concurrent-"+strconv.FormatBool(unreliable), unreliable)
	defer tc.cleanup()

	for i := 0; i < len(tc.groups); i++ {
		tc.join(i)
	}

	rr := rand.New(rand.NewSource(int64(os.Getpid())))
	const npara = 11
	var ca [npara]chan bool
	for i := 0; i < npara; i++ {
		ca[i] = make(chan bool)
		go func(me int) {
			ok := true
			defer func() { ca[me] <- ok }()
			ck := tc.clerk()
			mymck := tc.shardclerk()
			key := strconv.Itoa(me)
			last := ""
			for iters := 0; iters < 3; iters++ {
				nv := strconv.Itoa(rr.Int())
				ck.Append(key, nv)
				last = last + nv
				v := ck.Get(key)
				if v != last {
					ok = false
					t.Fatalf("Get(%v) expected %v got %v\n", key, last, v)
				}

				time.Sleep(time.Duration(rr.Int()%30) * time.Millisecond)

				gi := rr.Int() % len(tc.groups)
				gid := tc.groups[gi].gid
				mymck.Move(rr.Int()%common.NShards, gid)
			}
		}(i)
	}

	for i := 0; i < npara; i++ {
		select {
		case x := <-ca[i]:
			if x == false {
				t.Fatalf("something is wrong")
			}
		case <-time.After(30 * time.Second):
			t.Fatalf("Parallel client %d did not complete", i)
		}
	}
}

func TestConcurrent(t *testing.T) {
	fmt.Printf("Test: Concurrent Put/Get/Move ...\n")
	doConcurrent(t, false)
	fmt.Printf("  ... Passed\n")
}

func TestConcurrentUnreliable(t *testing.T) {
	fmt.Printf("Test: Concurrent Put/Get/Move (unreliable) ...\n")
	doConcurrent(t, true)
	fmt.Printf("  ... Passed\n")
}

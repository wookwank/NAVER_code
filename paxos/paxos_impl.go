package paxos

import (
	"math"
	"strconv"
	"time"

	"umich.edu/eecs491/proj4/common"
)

//
// channels for confinement
//
type Prepare struct {
	args 	PrepareArgs
	reply	*PrepareReply
	done	chan error
}

type Accept struct {
	args 	AcceptArgs
	reply	*AcceptReply
	done	chan error
}

type Inform struct {
	args 	InformArgs
	reply	*InformReply
	done	chan error
}

type Done struct {
	peerID	int
	seq		int
	v 		chan int
}

type Max struct {
	seq 	int
	max 	chan int
}

type Min struct {
	min		chan int
}

type Status struct {
	seq 	int
	fate 	chan Fate
	v 		chan interface{}
}

type Decision struct {
	seq			int
	newFate		Fate
	decision 	chan Fate
}

type MaxProposal struct {
	seq			int
	proposal 	int
	v 			chan int
}

type MinSeq struct {
	seq		int
	v		chan int
}

//
// additions to Paxos state.
//
type PaxosImpl struct {
	log 			map[int]PaxosState	// seq # --> paxos state
	decisionLog		map[int]Fate		// seq # --> decision state
	doneMap			map[int]int 		// peer # --> max done for this node			
	proposalMap 	map[int]int			// seq # --> Maximum proposal # seen for this sequence
	maxSequence 	int					// Maximum sequence # seen to this node
	minSeq			int					// Latest return value from min()

	prepChan 		chan Prepare
	acceptChan		chan Accept
	informChan		chan Inform
	decisionChan	chan Decision
	proposalChan 	chan MaxProposal
	doneChan		chan Done
	maxChan			chan Max
	minChan			chan Min
	minseqChan		chan MinSeq
	statusChan		chan Status
	end 			chan interface{}
}

type PaxosState struct {
	N_p		int
	N_a 	int
	V_a 	interface{}
}

//
// your px.impl.* initializations here.
//
func (px *Paxos) initImpl() {
	// Initialize essential data structures for Paxos instance
	px.impl.log = make(map[int]PaxosState)
	px.impl.decisionLog = make(map[int]Fate)
	px.impl.doneMap = make(map[int]int)
	px.impl.proposalMap = make(map[int]int)
	px.impl.maxSequence = -1
	px.impl.minSeq = 0

	// Initialize `doneMap` with -1 for all peers, indicating no completed sequence
    for i := 0; i < len(px.peers); i++ {
        px.impl.doneMap[i] = -1
    }

	// Initialize channels to handle different Paxos RPCs and operations
	px.impl.prepChan = make(chan Prepare)
	px.impl.acceptChan = make(chan Accept)
	px.impl.informChan = make(chan Inform)
	px.impl.decisionChan = make(chan Decision)
	px.impl.proposalChan = make(chan MaxProposal)

	px.impl.doneChan = make(chan Done)
	px.impl.maxChan = make(chan Max)
	px.impl.minChan = make(chan Min)
	px.impl.minseqChan = make(chan MinSeq)
	px.impl.statusChan = make(chan Status)
	px.impl.end = make(chan interface{})

	// Goroutine to monitor for termination of the Paxos instance
	go func() {
		defer close(px.impl.end)
		for !px.isdead() { time.Sleep(1000) }
	}()

	// Log managing Goroutine: Handles Prepare, Accept, Inform, and Status RPCs
	go func() {
		for {
			select{
			case prepReq := <-px.impl.prepChan:
				// Handle Prepare request
				err := px.handlePrepare(prepReq.args, prepReq.reply)
				prepReq.done <- err

			case acceptReq := <-px.impl.acceptChan:
				// Handle Accept request
				err := px.handleAccept(acceptReq.args, acceptReq.reply)
				acceptReq.done <- err

			case informReq := <-px.impl.informChan:
				// Handle Inform request
				err := px.handleInform(informReq.args, informReq.reply)
				informReq.done <- err
			
			case statusReq := <-px.impl.statusChan:
				// Handle Status request
				fate, v := px.handleStatus(statusReq.seq)
				statusReq.fate <- fate
				statusReq.v <- v
			
			case minReq := <-px.impl.minChan:
				// Handle Min request
				minReq.min <- px.handleMin()

			case <-px.impl.end:
				// Exit on termination
				return
			}
		}
	}()

	// Decision state managing Goroutine: Manages the decision log
	go func() {
		for {
			select {
			case stateReq := <- px.impl.decisionChan:
				// Update decision state with new fate if provided
				if stateReq.newFate != -1 {
					px.impl.decisionLog[stateReq.seq] = stateReq.newFate
				}
				stateReq.decision <- px.impl.decisionLog[stateReq.seq]
			
			case <-px.impl.end:
				// Exit on termination
				return
			}
		}
	}()

	// maxSequence managing Goroutine: Keeps track of the maximum sequence number
	go func() {
		for {
			select {
			case maxReq := <-px.impl.maxChan:
				// Update and return the maximum sequence number seen so far
				px.impl.maxSequence = max(maxReq.seq, px.impl.maxSequence)
				maxReq.max <- px.impl.maxSequence
			
			case <-px.impl.end:
				// Exit on termination
				return
			}
		}
	}()

	// maxProposal managing Goroutine: Manages the highest proposal number per sequence
	go func() {
		for {
			select {
			case proposalReq := <-px.impl.proposalChan:
				if proposalReq.proposal != -1 {
					// Update the highest proposal number if a new one is provided
					px.impl.proposalMap[proposalReq.seq] = max(px.impl.proposalMap[proposalReq.seq],proposalReq.proposal)
				} else {
					// Increment the proposal number for the given sequence
					px.impl.proposalMap[proposalReq.seq]++
				}
				// Return the updated proposal number
				proposalReq.v <- px.impl.proposalMap[proposalReq.seq]
			
			case <-px.impl.end:
				// Exit on termination
				return
			}
		}
	}()

	// doneMap managing Goroutine: Updates the highest done sequence number for each peer
	go func() {
		for {
			select {
			case doneReq := <-px.impl.doneChan:
				// Update doneMap with the highest completed sequence number
				px.impl.doneMap[doneReq.peerID] = max(px.impl.doneMap[doneReq.peerID], doneReq.seq)
				doneReq.v <- px.impl.doneMap[doneReq.peerID]
			
			case <-px.impl.end:
				// Exit on termination
				return
			}
		}
	}()

	// minSeq managing Goroutine: Manages the minimum sequence number (`minSeq`)
	go func() {
		for {
			select {
			case minseqReq := <-px.impl.minseqChan:
				// Update minSeq if a new minimum is provided
				if minseqReq.seq != -1 {
					px.impl.minSeq = minseqReq.seq
				}
				minseqReq.v <- px.impl.minSeq
			case <-px.impl.end:
				// Exit on termination
				return
			}
		}
	}()
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	// Launch a goroutine for the proposal process
	go func() {
		// Ignore if called with a seq # less than Min()
		if seq < px.update_get_min_sequence(-1) {
			return
		}

		// Update the maximum sequence number known to this peer
		px.update_get_max_sequence(seq)

		// Continue until a decision is reached or the Paxos instance is terminated
		for px.get_update_decision_state(seq, -1) != Decided && !px.isdead() {
			// Generate a new proposal (ballot) number for this sequence
			num_ballot := px.new_proposal_num(seq)

			// Phase 1: Prepare
			// Construct the Prepare request arguments
			sendArg := PrepareArgs{seq, num_ballot, px.me, px.update_get_done(px.me, -1)}

			// Send the Prepare request to peers and collect their responses
			// Returns:
			// - `attention`: Number of responses received (attention)
			// - `status`: Whether the sequence is already decided
			// - `decidedVal`: The decided value, if already decided
			attention, status, decidedVal := px.sendPrepare(sendArg, &v)

			// If a decision has already been made for this sequence
			if status == Decided {
				// Phase 3 (Immediate): Inform phase
				// Notify other peers of the decision for optimization
				informArg := InformArgs{seq, decidedVal, px.me, px.update_get_done(px.me, -1)}
				px.sendInform(informArg)
				break

			} else if attention <= len(px.peers) / 2 {
				// Try again if no majority attention
				continue
			} 

			// Phase 2: Accept phase
			// Construct the Accept request arguments
			acceptArg := AcceptArgs{seq, num_ballot, v, px.me, px.update_get_done(px.me, -1)}

			// Send the Accept request to peers and count their acceptance
			agreement := px.sendAccept(acceptArg)

			// If a majority of peers accepted the proposal
			if agreement > len(px.peers) / 2 {
				// Phase 3: Inform phase
				// Notify other peers of the agreed value
				informArg := InformArgs{seq, v, px.me, px.update_get_done(px.me, -1)}
				px.sendInform(informArg)
			}
		}
		// Update the minimum sequence number across all peers
		px.Min()
	}()
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	px.update_get_done(px.me, seq)
}

// update_get_done updates the 'done' data for a given peer and returns that value 
func (px *Paxos) update_get_done(peerID int, done int) int {
	req := Done{
		peerID: peerID,
		seq: done,
		v: make(chan int),
	}
	select{
	case px.impl.doneChan <- req:
		return <-req.v
	case <-px.impl.end:
		return -1
	}
}


//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	return px.update_get_max_sequence(-1)
}

// update_get_max_sequence updates the maximum sequence known to this peer.
func (px *Paxos) update_get_max_sequence(seq int) int {
	maxReq := Max{
		seq: seq,
		max: make(chan int),
	}
	select {
	case px.impl.maxChan <- maxReq:
		return <-maxReq.max
	case <-px.impl.end:
		return -1
	}
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peer's z_i is -1 if it has
// never called Done().
//
func (px *Paxos) Min() int {
	minReq := Min{
		min: make(chan int),
	}
	select {
	case px.impl.minChan <- minReq:
		return <-minReq.min
	case <-px.impl.end:
		return -1
	}
}

func (px *Paxos) handleMin() int {
	// Iterate over all peers to find the minimum z_i in the doneMap
	min := math.MaxInt32
	for peerId := range px.peers {
		done := px.update_get_done(peerId, -1)
		if done < min {
			min = done
		}
	}

	// Forget log entries whose sequence numbers are less than or equal to min
	prevMinSeq := px.update_get_min_sequence(-1)
	for i := prevMinSeq; i <= min; i++ {
		delete(px.impl.log, i)
	}

	// Update the minimum sequence to one more than the found minimum
	updateMinSeq := px.update_get_min_sequence(min + 1)
	return updateMinSeq
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	statusReq := Status{
		seq: seq,
		fate: make(chan Fate),
		v: make(chan interface{}),
	}

	select {
	case px.impl.statusChan <- statusReq:
		return <-statusReq.fate, <-statusReq.v
	case <-px.impl.end:
		return Pending, nil
	}
}

func (px *Paxos) handleStatus(seq int) (Fate, interface{}) {
	// Get the decision state of the given sequence
	decision := px.get_update_decision_state(seq, -1)
	// Get the Paxos state (proposal information) of the given sequence
	state := px.get_paxos_state(seq)

	if seq < px.update_get_min_sequence(-1) {
		// Return `Forgotten` if the sequence number is less than the current Min().
		return Forgotten, nil
	} else if decision == Decided {
		// If the decision state is `Decided`, return `Decided` with the agreed value.
		return Decided, state.V_a
	} else {
		// Otherwise, the sequence is still `Pending`, and no value is agreed upon yet
		return Pending, nil
	}
}


///////////////////////////// RPC Calls ////////////////////////////

func (px *Paxos) Prepare(arg PrepareArgs, reply *PrepareReply) error {
	prepReq := Prepare{
		args: arg,
		reply: reply,
		done: make(chan error),
	}

	select {
	case px.impl.prepChan <- prepReq:
		err := <- prepReq.done
		return err
	case <-px.impl.end:
		return nil
	}
}

func (px *Paxos) handlePrepare(arg PrepareArgs, reply *PrepareReply) error {
	// Update 'done' map and max known sequence for the proposer
	px.update_get_done(arg.PeerID, arg.Done)
	px.update_get_max_sequence(arg.Seq)

	// Retrieve or initialize the Paxos state for the sequence
	state := px.get_paxos_state(arg.Seq)

	// Check if the sequence is already decided
	if px.get_update_decision_state(arg.Seq, -1) == Decided {
		// Respond with Reject, highest proposal seen, and decided value
		reply.Reply = Reject
		reply.N_p = px.get_update_proposal_num(arg.Seq, -1) 
		reply.N_a = -1
		reply.V_a = px.impl.log[arg.Seq].V_a
	} 
	
	// Check and update the highest promised proposal number (N_p)
	if arg.N > state.N_p {
		state.N_p = arg.N
		px.impl.log[arg.Seq] = state
		reply.Reply = OK
		reply.N_p, reply.N_a, reply.V_a = state.N_p, state.N_a, state.V_a
	} else {
		// Reject the Prepare request if `N` is not higher
		reply.Reply = Reject
		reply.N_p = px.get_update_proposal_num(arg.Seq, -1) // Return the highest proposal seen
	}

	// Include peer ID and highest `done` sequence in the response
	reply.PeerID = px.me
	reply.Done = px.update_get_done(px.me, -1)

	return nil
}

func (px *Paxos) Accept(arg AcceptArgs, reply *AcceptReply) error {
	acceptReq := Accept{
		args: arg,
		reply: reply,
		done: make(chan error),
	}

	select {
	case px.impl.acceptChan <- acceptReq:
		err := <- acceptReq.done
		return err
	case <-px.impl.end:
		return nil
	}
}

func (px *Paxos) handleAccept(arg AcceptArgs, reply *AcceptReply) error {
	// Update 'done' map and max known sequence for the proposer
	px.update_get_done(arg.PeerID, arg.Done)
	px.update_get_max_sequence(arg.Seq)

	// Retrieve or initialize the Paxos state for this sequence
	state := px.get_paxos_state(arg.Seq)

	// Accept the proposal if proposal number N is at least the highest promised (N_p)
	if arg.N >= state.N_p {
		// Update the log
		state.N_p, state.N_a, state.V_a = arg.N, arg.N, arg.Value
		px.impl.log[arg.Seq] = state

		// Respond with OK and the updated state information
		reply.Reply = OK
		reply.N_p, reply.N_a, reply.V_a = state.N_p, state.N_a, state.V_a
	} else {
		// Reject if the proposal number `N` is not high enough
		reply.Reply = Reject
		reply.N_p = px.get_update_proposal_num(arg.Seq, -1) // The highest proposal seen so far
	}

	// Include peer ID and highest `done` sequence in the response
	reply.PeerID = px.me
	reply.Done = px.update_get_done(px.me, -1)

	return nil
}

func (px *Paxos) Inform(arg InformArgs, reply *InformReply) error {
	informReq := Inform{
		args: arg,
		reply: reply,
		done: make(chan error),
	}

	select {
	case px.impl.informChan <- informReq:
		err := <- informReq.done
		return err
	case <-px.impl.end:
		return nil
	}
}

func (px *Paxos) handleInform(arg InformArgs, reply *InformReply) error {
	// Update 'done' map and max known sequence for the proposer
	px.update_get_done(arg.PeerID, arg.Done)
	px.update_get_max_sequence(arg.Seq)

	// Retrieve or initialize the Paxos state for this sequence
	state := px.get_paxos_state(arg.Seq)

	// Mark the sequence as Decided with the provided value
	state.V_a = arg.Value
	px.get_update_decision_state(arg.Seq, Decided)
	px.impl.log[arg.Seq] = state

	// Prepare the response indicating success
	reply.Reply = OK
	reply.PeerID = px.me
	reply.Done = px.update_get_done(px.me, -1)

	return nil
}

//////////////////////// Helper functions ////////////////////////////

func (px *Paxos) sendPrepare(arg PrepareArgs, V_a *interface{}) (int, Fate, interface{}){
	// Initialize tracking variables
	attention, maxN_a := 0, -1
	status := Pending
	majorityMap := make(map[interface{}]int)
	var decidedVal interface{}
	
	// Iterate through all peers to send Prepare RPCs
	for i := 0; i < len(px.peers); i++ {
		var rep PrepareReply
		rpcOk := true

		// Call Prepare locally or remotely
		if i == px.me {
			px.Prepare(arg, &rep)
		} else {
			rpcOk = common.Call(px.peers[i], "Paxos.Prepare", arg, &rep)
		}

		// Process only successful RPCs
		if rpcOk {
			// Update 'done' data of peers
			px.update_get_done(rep.PeerID, rep.Done)

			// If the Prepare is accepted (OK response)
			if rep.Reply == OK {
				attention++

				// Track accepted values for potential majority consensus
				if rep.N_a > -1 {
					majorityMap[rep.N_a]++
					if majorityMap[rep.N_a] > len(px.peers)/2 {
						decidedVal = rep.V_a
						status = Decided
						break
					}
				}

				// Update the highest accepted proposal value
				if rep.V_a != nil && rep.N_a > maxN_a {
					maxN_a = rep.N_a
					*V_a = rep.V_a
				}

			} else if rep.Reply == Reject {
				// If rejected, update the proposal number based on the highest seen
				px.get_update_proposal_num(arg.Seq, rep.N_p)

				// If the sequence is already decided, capture the decided value
				if rep.N_a == -1 {
					decidedVal = rep.V_a
					status = Decided
				}
			}
		}
	}

	return attention, status, decidedVal
}

func (px *Paxos) sendAccept(arg AcceptArgs) int {
	agreement := 0

	// Iterate through all peers to send Accept RPCs
	for i := 0; i < len(px.peers); i++ {
		var rep AcceptReply
		rpcOk := true

		// Call Accept locally or remotely
		if i == px.me {
			px.Accept(arg, &rep)
		} else {
			rpcOk = common.Call(px.peers[i], "Paxos.Accept", arg, &rep)
		}

		if rpcOk {
			// Update 'done' data of peers
			px.update_get_done(rep.PeerID, rep.Done)

			// If the Accept is successful (OK response), increment agreement count
			if rep.Reply == OK {
				agreement++
			} else if rep.Reply == Reject {
				// If rejected, update the proposal number based on the highest seen
				px.get_update_proposal_num(arg.Seq, rep.N_p)
			}
		}
	}
	return agreement
}

func (px *Paxos) sendInform(arg InformArgs) {
	// Iterate through all peers to send Inform RPCs
	for i := 0; i < len(px.peers); i++ {
		var rep InformReply
		rpcOk := true

		// Call Inform locally or remotely
		if i == px.me {
			px.Inform(arg, &rep)
		} else {
			rpcOk = common.Call(px.peers[i], "Paxos.Inform", arg, &rep)
		}

		if rpcOk {
			// Update the 'done' data of peers
			px.update_get_done(rep.PeerID, rep.Done)
		}
	}
}

// get_update_proposal_num updates the proposal number for a given sequence.
func (px *Paxos) get_update_proposal_num(seq int, proposal int) int {
	req := MaxProposal{
		seq: seq,
		proposal: proposal,
		v: make(chan int),
	}
	select {
	case px.impl.proposalChan <- req:
		return <-req.v
	case <-px.impl.end:
		return -1
	}
}

// new_proposal_num generates a new unique proposal number for a given sequence.
func (px *Paxos) new_proposal_num(seq int) int {
	proposalNum := px.get_update_proposal_num(seq, -1)
	aStr := strconv.Itoa(proposalNum)
	bStr := strconv.Itoa(px.me)

	concatenated, _ := strconv.Atoi(aStr + bStr)
	return concatenated
}

// get_paxos_state retrieves or initializes the Paxos state for a given sequence number.
func (px *Paxos) get_paxos_state(seq int) PaxosState {
	state, exists := px.impl.log[seq]
	if !exists {
		// Initialize state if this is the first time accessing this sequence
		state = PaxosState{
			N_p:    -1,
			N_a:    -1,
			V_a:    -1,
		}
		px.impl.log[seq] = state
	}
	return state
}

// get_update_decision_state updates the decision (fate) for a given sequence and returns the updated fate.
func (px *Paxos) get_update_decision_state(seq int, fate Fate) (Fate) {
	req := Decision{
		seq: seq, 
		newFate: fate, 
		decision: make(chan Fate),
	}

	select {
	case px.impl.decisionChan <- req:
		return <-req.decision
	case <-px.impl.end:
		return Pending
	}
}

// update_get_min_sequence retrieves or updates the minimum sequence number for garbage collection purposes.
func (px *Paxos) update_get_min_sequence(seq int) int {
	req := MinSeq{
		seq: seq,
		v: make(chan int),
	}
	select {
	case px.impl.minseqChan <- req:
		return <-req.v
	case <-px.impl.end:
		return -1
	}
}

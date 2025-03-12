package paxosrsm

import (
	"time"

	"umich.edu/eecs491/proj4/paxos"
)

//
// additions to PaxosRSM state
//
type PaxosRSMImpl struct {
	nextSeq		int
}

//
// initialize rsm.impl.*
//
func (rsm *PaxosRSM) InitRSMImpl() {
	rsm.impl.nextSeq = 0
}

//
// application invokes AddOp to submit a new operation to the replicated log
// AddOp returns only once value v has been decided for some Paxos instance
//
func (rsm *PaxosRSM) AddOp(v interface{}) {
	for {
		// Check the status of the current Paxos instance sequence `rsm.impl.nextSeq`.
		status, storedV := rsm.px.Status(rsm.impl.nextSeq)

		if status == paxos.Decided {
			// If already decided, apply the operation and move to the nextSeq instance.
			rsm.applyOp(storedV)
			rsm.px.Done(rsm.impl.nextSeq)
			rsm.impl.nextSeq++

			// Duplicate request. Return if the decided value matches the current request `v`
			if rsm.equals(storedV, v) {
				return
			}
		} else if status == paxos.Pending {
			// If the instance is pending, start proposing the value `v`.
			rsm.px.Start(rsm.impl.nextSeq, v)
			
			// Exponential backoff for retrying until a decision is made.
			to := 10 * time.Millisecond
			for {
				// Recheck the status of the current Paxos instance.
				status, storedV := rsm.px.Status(rsm.impl.nextSeq)
				
				if status == paxos.Decided {
					// If the value has been decided:
					if rsm.equals(storedV, v) {
						// If it matches the value `v`, apply the operation and mark it as done.
						rsm.applyOp(storedV)
						rsm.px.Done(rsm.impl.nextSeq)
						rsm.impl.nextSeq++
						return
					} else {
						// If a different value was decided, break and retry with the next sequence.
						break
					}
				}

				// Exponential backoff mechanism, doubling the timeout up to 500ms.
				time.Sleep(to)
				if to < 500 * time.Millisecond {
					to *= 2
				}
			}
		}
	}
}
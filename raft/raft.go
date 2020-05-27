// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"math/rand"
	"time"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// number of ticks since it reached last electionTimeout
	electionElapsed int
	// the randomized electionTimeout that we should wait for
	realElectionTimeout int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	rand *rand.Rand
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	hardState, confState, err := c.Storage.InitialState()
	if err != nil {
		panic(err.Error())
	}
	if len(confState.Nodes) > 0 && len(c.peers) > 0 {
		panic("Cannot override peers when recover")
	}
	nodes := confState.GetNodes()
	if len(nodes) == 0 {
		nodes = c.peers
	}
	RaftLog := newLog(c.Storage)
	Prs := make(map[uint64]*Progress)
	votes := make(map[uint64]bool)
	for _, node := range nodes {
		Prs[node] = &Progress{
			Next:  RaftLog.LastIndex(),
			Match: 0,
		}
		votes[node] = false
	}
	return &Raft{
		id:               c.ID,
		Term:             hardState.GetTerm(),
		Vote:             hardState.GetVote(),
		RaftLog:          RaftLog,
		Prs:              Prs,
		State:            StateFollower,
		votes:            votes,
		msgs:             []pb.Message{},
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		rand:             rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	}
	r.msgs = append(r.msgs, msg)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Index:   0,
		LogTerm: 0,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) bcastBeat() {
	for peer := range r.Prs {
		if peer != r.id {
			r.sendHeartbeat(peer)
		}
	}
	r.heartbeatElapsed = 0
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	switch r.State {
	case StateFollower:
		fallthrough
	case StateCandidate:
		r.electionElapsed += 1
		if r.electionElapsed > r.realElectionTimeout {
			r.electionElapsed = 0
			r.becomeCandidate()
			r.startElection()
		}
	case StateLeader:
		r.heartbeatElapsed += 1
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.bcastBeat()
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Lead = lead
	if term > r.Term {
		r.Term = term
		r.Vote = None
	}
	r.electionElapsed = 0
	r.randomizeElectionTimeout()
}

func (r *Raft) voteGranted(from uint64) {
	r.votes[from] = true
	numVotes := 0
	for _, voted := range r.votes {
		if voted {
			numVotes++
		}
	}
	if numVotes > len(r.Prs)/2 {
		r.becomeLeader()
	}
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	r.State = StateCandidate
	r.Term++
	r.Vote = r.id
	r.voteGranted(r.id)
	r.electionElapsed = 0
	r.randomizeElectionTimeout()
}

func (r *Raft) startElection() {
	for peer := range r.votes {
		lastIdx := r.RaftLog.LastIndex()
		myTerm, err := r.RaftLog.Term(lastIdx)
		if err != nil {
			panic(err)
		}
		if peer != r.id {
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgRequestVote,
				To:      peer,
				From:    r.id,
				Term:    r.Term,
				LogTerm: myTerm,
				Index:   lastIdx,
			})
		}
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{
		EntryType: pb.EntryType_EntryNormal,
		Term:      r.Term,
		Index:     r.RaftLog.LastIndex() + 1,
	})
}

func (r *Raft) randomizeElectionTimeout() {
	r.realElectionTimeout = r.electionTimeout + r.rand.Intn(r.electionTimeout)
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	if m.GetTerm() > r.Term {
		r.becomeFollower(m.GetTerm(), 0)
	}
	switch m.GetMsgType() {
	case pb.MessageType_MsgAppend:
		fallthrough
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResponse(m)
	case pb.MessageType_MsgHup:
		if r.State != StateLeader {
			r.becomeCandidate()
			r.startElection()
		}
	case pb.MessageType_MsgBeat:
		if r.State == StateLeader {
			r.bcastBeat()
		}
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	switch r.State {
	case StateFollower:
		r.electionElapsed = 0
		// replicate log here
		if m.GetCommit() > r.RaftLog.committed {
			r.RaftLog.committed = m.GetCommit()
		}
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgHeartbeatResponse,
			Term:    r.Term,
			From:    r.id,
			To:      m.From,
		})
	case StateCandidate:
		if m.GetTerm() >= r.Term {
			r.becomeFollower(m.GetTerm(), m.GetFrom())
		}
	case StateLeader:
		if m.GetTerm() > r.Term {
			r.becomeFollower(m.GetTerm(), m.GetFrom())
		}
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	r.handleAppendEntries(m)
}

func (r *Raft) handleRequestVote(m pb.Message) error {
	var grantVote bool
	if m.GetTerm() < r.Term {
		grantVote = false
	} else {
		theirLastLogIdx := m.GetIndex()
		theirLastLogTerm := m.GetLogTerm()
		ourLastLogIdx := r.RaftLog.LastIndex()
		ourLastLogTerm, err := r.RaftLog.Term(ourLastLogIdx)
		if err != nil {
			return err
		}
		if (r.Vote == m.From || r.Vote == None) && !(ourLastLogTerm > theirLastLogTerm || (ourLastLogTerm == theirLastLogTerm && ourLastLogIdx > theirLastLogIdx)) {
			r.Vote = m.From
			grantVote = true
		} else {
			grantVote = false
		}
	}
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Reject:  !grantVote,
	})
	return nil
}

func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	if !m.Reject {
		r.voteGranted(m.From)
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
	r.votes[id] = false
	r.Prs[id] = &Progress{
		Next:  r.RaftLog.LastIndex() + 1,
		Match: 0,
	}
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
	delete(r.votes, id)
	delete(r.Prs, id)
}

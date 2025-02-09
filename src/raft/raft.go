package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"fmt"
        //	"math/rand"
        //	"math/big"
	// 	crand "crypto/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

const HeartbeatInterval time.Duration = 15 * time.Millisecond
var cnt int = 0
var cnt_mu sync.Mutex         
// return a rand from 53 to 660
func GenEleInterval() time.Duration {
	cnt_mu.Lock()
	defer cnt_mu.Unlock()
	d := time.Duration(53+cnt*57) * time.Millisecond
	cnt = (cnt + 1) % 7 
	return d
}

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	votemu    sync.Mutex          // Lock to protect shared access to this peer's vote state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	
	isLeader        bool
	heartbeatTimer  *time.Timer
	electionTimer   *time.Timer

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.isLeader
}

func (rf *Raft) SetTerm(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm = term
}

func (rf *Raft) NextTerm() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm += 1
}

func (rf *Raft) SetLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.isLeader = true
}

func (rf *Raft) SetFollow() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.isLeader = false
}

func (rf *Raft) GetVotedFor() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.votedFor	
}

func (rf *Raft) SetVotedFor(id int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.votedFor = id
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term int
	CandidateId int
	// lastLogIndex int
	// lastLogTerm int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	// fmt.Printf("id %d current %d args_term %d args_candidate %d\n", rf.me, rf.currentTerm, args.Term, args.CandidateId)
	reply.Term = args.Term
	term, isleader := rf.GetState()
	if term< args.Term {
		rf.SetTerm(args.Term)
		rf.SetVotedFor(args.CandidateId)
		rf.SetFollow()
		rf.electionTimer.Reset(GenEleInterval())
		reply.VoteGranted = true
		return
	} else if term == args.Term {
		votedfor := rf.GetVotedFor()
		if isleader || votedfor != args.CandidateId {
			reply.VoteGranted = false
			return
		} else {
			reply.VoteGranted = true
			rf.electionTimer.Reset(GenEleInterval())
			return
		}
	} else {
		reply.Term,_ = rf.GetState()
		reply.VoteGranted = false
		return 
	}
}

type HeartbeatArgs struct {
	Term int
}

type HeartbeatReply struct {
	Term int
	Received bool
}

func (rf *Raft) HeartBeat(args *HeartbeatArgs, reply *HeartbeatReply) {
	// fmt.Printf("id %d curTerm %d argsTerm %d\n", rf.me, rf.currentTerm, args.Term)
	term,_ := rf.GetState()
	if term == args.Term {
		reply.Term = term
		reply.Received = rf.electionTimer.Reset(GenEleInterval())
		return
	} else if term < args.Term {
		rf.SetFollow()
		rf.SetTerm(args.Term)
		reply.Term = args.Term
		reply.Received = rf.electionTimer.Reset(GenEleInterval())
		return
	} else {
		reply.Term = term
		reply.Received = false
		return 
	}

}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}


// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	// Your code here (3B).


	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed()  {

		// Your code here (3A)
		// Check if a leader election should be started.
		select {
			case <-rf.electionTimer.C: 
				rf.NextTerm()
				// fmt.Printf("id %d term %d elect\n", rf.me, rf.currentTerm)
				rf.elect()
				rf.electionTimer.Reset(GenEleInterval())
			case <-rf.heartbeatTimer.C:
				_,isleader := rf.GetState()
				if isleader {
					rf.heartbeatBoardcast()
					rf.heartbeatTimer.Reset(HeartbeatInterval)
					rf.electionTimer.Reset(GenEleInterval())
				}
		}	
	}
}

func (rf *Raft) elect() {
	voted := 1
	rf.SetVotedFor(rf.me)
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int){
			term,isleader := rf.GetState()
			args := &RequestVoteArgs{}
			args.Term = term
			args.CandidateId = rf.me
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(server, args, reply)
			if ok && reply.Term == term && reply.VoteGranted {
				rf.votemu.Lock()
				voted += 1
				if voted > len(rf.peers)/2 && !isleader {
					rf.SetLeader()
					fmt.Printf("%d is leader\n", rf.me)
					rf.heartbeatBoardcast()
					rf.heartbeatTimer.Reset(HeartbeatInterval)
					rf.electionTimer.Reset(GenEleInterval())
				}
				rf.votemu.Unlock()
			}
		}(server)
	}
}

func (rf *Raft) heartbeatBoardcast() {
	// fmt.Printf("id %d term %d heartbeat\n", rf.me, rf.currentTerm)
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int){
			args := &HeartbeatArgs{}
			term,_ := rf.GetState()
			args.Term = term
			reply := &HeartbeatReply{}
			ok := rf.peers[server].Call("Raft.HeartBeat", args, reply)
			// fmt.Printf("id %d currTerm %d argsTerm %d\n", rf.me, term, reply.Term)
			if ok && reply.Term > term {
				rf.SetFollow()
			}
		}(server)
	}
	return 
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.isLeader = false
	rf.heartbeatTimer = time.NewTimer(HeartbeatInterval)
	rf.electionTimer = time.NewTimer(GenEleInterval())
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}

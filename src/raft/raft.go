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

	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state           State
	lowBoundTimeout int64 // ns
	upBoundTimeout  int64 // ns
	applyCh         chan ApplyMsg

	// TODO
	isHeartbeated bool
	isGranted     bool
	timeout       time.Time

	// Persistent state on all servers
	currentTerm int      // latest term server has seen
	votedFor    int      // candidateId that received vote in current term (or -1 indecate none)
	log         []*Entry // log entries

	// Volatile state on all servers
	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine

	// Volatile state on leaders
	nextIndex  []int // for each server, index of the next log entry to send to that server
	matchIndex []int // for each server, index of highest log entry known to be replicated on server
}

type State int

const (
	follower State = iota
	candidate
	leader
)

func (st State) String() string {
	switch st {
	case follower:
		return "Follower"
	case candidate:
		return "Candidate"
	case leader:
		return "Leader"
	default:
		return "unknown"
	}
}

type Entry struct {
	Term    int
	Command interface{}
}

func NewEntry(term int, command interface{}) *Entry {
	return &Entry{Term: term, Command: command}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == leader
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []*Entry
	if err := d.Decode(&currentTerm); err != nil {
		DPrintln("error in readPersist decoding currentTerm:", err)
		return
	} else if err = d.Decode(&votedFor); err != nil {
		DPrintln("error in readPersist decoding votedFor:", err)
		return
	} else if err = d.Decode(&log); err != nil {
		DPrintln("error in readPersist decoding log:", err)
		return
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

type AppendEntriesArgs struct {
	Term         int      // leader's term
	LeaderId     int      // so follower can redirect clients
	PrevLogIndex int      // index of log entry immediately preceding new ones
	PrevLogTerm  int      // term of prevLogIndex entry
	Entries      []*Entry // log entries to store (empty for heartbeat;may send more than one for efficiency)
	LeaderCommit int      // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintln(rf.me, "[Get RV RPC]", "current votefor:", rf.votedFor, "curterm:", rf.currentTerm,
		"lastLogTerm:", rf.log[len(rf.log)-1].Term,
		"arg.Term:", args.LastLogTerm,
		"lastLogIndex:", len(rf.log)-1,
		"arg.index:", args.LastLogIndex)

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false // 1. Reply false if term < currentTerm
		DPrintln(rf.me, "[Get RV RPC]", "vote False for", args.CandidateId, "cause this candidate's term:", args.Term, "< current term:", rf.currentTerm)
		return
	}

	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = follower
		rf.votedFor = -1
		rf.resetTimeout()
		rf.persist()
	}

	// 2. If votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver’s log, grant vote
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		lastLogTerm := rf.log[len(rf.log)-1].Term
		lastLogIndex := len(rf.log) - 1
		if args.LastLogTerm > lastLogTerm || args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.resetTimeout()
			rf.persist()
			return
		}
	}
	reply.VoteGranted = false
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false // 1. Reply false if term < currentTerm
		DPrintln(rf.me, "[Get AE RPC]", "reply False for", args.LeaderId, "cause this leader's term:", args.Term, "< current term:", rf.currentTerm)
		return
	}

	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}

	// if rf.state == candidate {
	// rf.state = follower // If AppendEntries RPC received from new leader: convert to follower
	// }
	rf.state = follower // If AppendEntries RPC received from new leader: convert to follower
	rf.resetTimeout()
	// DPrintln(rf.me, "[Hearbeat]", args.LeaderId, "->", rf.me, "sender term:", args.Term)

	// 2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	if len(rf.log) <= args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		DPrintln(rf.me, "[Get AE RPC]", "reply False for", args.LeaderId, "cause this leader's log doesn't match at prev index:", args.PrevLogIndex, "prev term:", args.PrevLogTerm)
		return
	}
	reply.Success = true

	i, j := args.PrevLogIndex+1, 0
	for i < len(rf.log) && j < len(args.Entries) {
		// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
		if rf.log[i].Term != args.Entries[j].Term {
			rf.log = rf.log[:i]
			break
		}
		i++
		j++
	}
	rf.log = append(rf.log, args.Entries[j:]...) // 4. Append any new entries not already in the log
	rf.persist()
	var logs []Entry
	for _, x := range rf.log {
		logs = append(logs, *x)
	}
	DPrintln(rf.me, "[Get AE RPC]", "leader:", args.LeaderId, "send term:", args.Term, "append log:", args.Entries[j:], "all logs", logs)

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit < len(rf.log)-1 {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = len(rf.log) - 1
		}
	}
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.lastApplied].Command,
			CommandIndex: rf.lastApplied,
		}
		// DPrintln(rf.me, ": [apply new commit]", "term:", rf.currentTerm, "commitIndex:", rf.commitIndex, "command:", rf.log[rf.lastApplied].Command)
	}
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	if rf.killed() {
		return index, term, false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	index = len(rf.log) // rf.log's first index is 1
	isLeader = rf.state == leader
	if isLeader {
		rf.log = append(rf.log, NewEntry(term, command))
		rf.persist()
		var logs []Entry
		for _, x := range rf.log {
			logs = append(logs, *x)
		}
		DPrintln(rf.me, "[start command]", "term:", term, "index:", index, "command:", command, "all logs:", logs)
	}
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		time.Sleep(time.Millisecond * 20)
		rf.mu.Lock()
		if rf.state != leader && rf.hasTimeouted() {
			rf.startNewElection()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) startNewElection() {
	DPrintln(rf.me, "convert to candidate", "of term:", rf.currentTerm+1)
	rf.state = candidate
	rf.currentTerm++    // 1. Increment currentTerm
	rf.votedFor = rf.me // 2. Vote for self
	rf.resetTimeout()   // 3. Reset election timer
	rf.persist()
	voteTerm := rf.currentTerm // record the Term before all sendRequestVoteRPCs
	args := &RequestVoteArgs{
		Term:         voteTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log),
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	var voteCnt int = 1
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		reply := &RequestVoteReply{}
		go func(i int) {
			if ok := rf.sendRequestVote(i, args, reply); ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.state != candidate {
					DPrintln(rf.me, "candidate state has changed to", rf.state, "when getting vote reply of ", i, " in term ", voteTerm)
					return
				}
				if reply.Term > voteTerm { // discover new term, revert to follower, and need to update currentTerm
					DPrintln(rf.me, "candidate receives new term", reply.Term, "from vote reply of ", i, " in term ", voteTerm)
					rf.convertToFollower(reply.Term)
					return
				}

				if reply.VoteGranted {
					DPrintln(rf.me, "voteGranted:", i, "->", rf.me, "term ", voteTerm)
					voteCnt++
					if voteCnt > len(rf.peers)/2 {
						DPrintln(rf.me, "[vote succeed]", rf.me, "-> leader of term", voteTerm, "voteCnt:", voteCnt)
						rf.convertToLeader()
					}
				} else {
					DPrintln(rf.me, "voteFailed:", i, "->", rf.me, "term ", voteTerm)
				}
			}
		}(i)
	}
}

func (rf *Raft) convertToFollower(term int) {
	rf.state = follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.resetTimeout()
	rf.persist()
	DPrintln(rf.me, "convert to follower of term ", term)
}

func (rf *Raft) convertToLeader() {
	rf.state = leader // if votes received from majority of servers: become leader
	DPrintln(rf.me, "convert to leader of term ", rf.currentTerm)

	go func() {
		for {
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				rf.mu.Lock()
				nextIndex := rf.nextIndex[i]
				args := &AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: nextIndex - 1,
					PrevLogTerm:  rf.log[nextIndex-1].Term,
					Entries:      rf.log[nextIndex:],
					LeaderCommit: rf.commitIndex,
				}
				rf.mu.Unlock()
				reply := &AppendEntriesReply{}
				go func(i int) {
					// t1 := time.Now()
					if ok := rf.sendAppendEntries(i, args, reply); ok {
						// d := time.Since(t1)
						// DPrintln("AE time duration:", d, "peer:", i)
						rf.mu.Lock()
						defer rf.mu.Unlock()
						DPrintln(rf.me, "[Get AE reply]", "peer:", i, "term:", rf.currentTerm, "nextIndex:", args.PrevLogIndex+1, "matchIndex:", rf.matchIndex[i], "entry:", args.Entries)
						if rf.state != leader || rf.currentTerm != args.Term {
							return
						}
						if reply.Term > rf.currentTerm {
							DPrintln(rf.me, "leader get larger term id from AE reply:", rf.me, "<-", i, "leader.term:", rf.currentTerm, "reply.term:", reply.Term)
							rf.convertToFollower(reply.Term)
							return
						}
						if reply.Term == rf.currentTerm {
							if reply.Success {
								rf.nextIndex[i] = args.PrevLogIndex + len(args.Entries) + 1
								rf.matchIndex[i] = rf.nextIndex[i] - 1
								DPrintln(rf.me, "[Get AE succeed]", "peer:", i, "term:", rf.currentTerm, "nextIndex:", rf.nextIndex[i], "matchIndex:", rf.matchIndex[i])

								commitedIndex := rf.commitIndex
								for i := commitedIndex + 1; i < len(rf.log); i++ {
									if rf.log[i].Term == rf.currentTerm {
										count := 1
										for j := 0; j < len(rf.peers); j++ {
											if rf.matchIndex[j] >= i {
												count++
											}
										}
										if count > len(rf.peers)/2 {
											commitedIndex = i
										}
									}
								}
								if rf.commitIndex != commitedIndex {
									rf.commitIndex = commitedIndex
									for rf.lastApplied < rf.commitIndex {
										rf.lastApplied++
										rf.applyCh <- ApplyMsg{
											CommandValid: true,
											Command:      rf.log[rf.lastApplied].Command,
											CommandIndex: rf.lastApplied,
										}
										DPrintln(rf.me, ": [apply new commit]", "term:", rf.currentTerm, "commitIndex:", rf.commitIndex, "command:", rf.log[rf.lastApplied].Command)
									}
								}
							} else {
								rf.nextIndex[i]--
								DPrintln(rf.me, "[Get AE failed]", "peer:", i, "term:", rf.currentTerm, "nextIndex:", rf.nextIndex[i], "matchIndex:", rf.matchIndex[i])
							}
						}
					}
				}(i)
			}
			time.Sleep(time.Millisecond * 50)
			rf.mu.Lock()
			if rf.state != leader {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
		}
	}()
}

func (rf *Raft) resetTimeout() {
	interval := rf.upBoundTimeout - rf.lowBoundTimeout + 1
	ns := rf.lowBoundTimeout + rand.Int63n(interval)
	rf.timeout = time.Now().Add(time.Duration(ns))
}

func (rf *Raft) hasTimeouted() bool {
	return time.Now().After(rf.timeout)
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = 1
	}

	rf.matchIndex = make([]int, len(peers))

	// Your initialization code here (2A, 2B, 2C).
	rf.state = follower
	rf.votedFor = -1                          // indicate null
	rf.log = append(rf.log, NewEntry(0, nil)) // append placeholder, the first valid log index is 1
	rf.lowBoundTimeout = 1000000 * 150
	rf.upBoundTimeout = 1000000 * 300
	rf.resetTimeout()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func init() {
	rand.Seed(time.Now().Unix())
	log.SetFlags(log.Lshortfile)
}

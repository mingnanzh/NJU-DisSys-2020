package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, Isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "labrpc"

import "fmt"
import "time"
import "math/rand"

// import "bytes"
// import "encoding/gob"

const (
	Leader    = 0
	Candidate = 1
	Follower  = 2
)

type Logs struct {
	Command interface{}
	Term    int
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	Mu        sync.Mutex
	Peers     []*labrpc.ClientEnd
	Persister *Persister
	Me        int // index into Peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	ApplyCh chan ApplyMsg

	// persistent state on all servers
	HeartBeatSignal bool
	State           int
	CurrentTerm     int
	VotedFor        int
	VoteCount       int
	Log             []Logs

	// volatile state on all servers
	CommitIndex int
	LastApplied int

	// volatile state on leaders (reinitialized after election)
	NextIndex  []int
	MatchIndex []int
}

func min(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

// return CurrentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var Term int
	var Isleader bool
	// Your code here.
	Term = rf.CurrentTerm
	Isleader = (rf.State == Leader)
	return Term, Isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.Persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int // candidate's Term
	CandidateID  int // candidate requesting vote
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int  // CurrentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

type AppendEntriesArgs struct {
	Term         int // leader's Term
	LeaderID     int // so follower can redirect clients
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Logs
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int  // CurrentTerm, for leader to update itself
	Success bool // true if follower contained entry matching PrevLogIndex and PrevLogTerm
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	switch state := rf.State; state {
	case Follower:
		{
			//fmt.Printf("ID %d (follower) received RequestVote RPC [%d,%d,%d,%d] in Term %d, Log:%v\n", rf.Me, args.Term, args.CandidateID, args.LastLogIndex, args.LastLogTerm, rf.CurrentTerm, rf.Log)
			if rf.CurrentTerm > args.Term {
				reply.Term = rf.CurrentTerm
				reply.VoteGranted = false
				//fmt.Printf("reject1\n")
			} else if (rf.CurrentTerm == args.Term) && (rf.VotedFor == -1 || rf.VotedFor == args.CandidateID) && (rf.Log[len(rf.Log)-1].Term < args.LastLogTerm || (rf.Log[len(rf.Log)-1].Term == args.LastLogTerm && len(rf.Log)-1 <= args.LastLogIndex)) {
				rf.CurrentTerm = reply.Term
				rf.VotedFor = args.CandidateID
				reply.Term = args.Term
				reply.VoteGranted = true
				//fmt.Printf("confirm1\n")
			} else if rf.CurrentTerm < args.Term && (rf.Log[len(rf.Log)-1].Term < args.LastLogTerm || (rf.Log[len(rf.Log)-1].Term == args.LastLogTerm && len(rf.Log)-1 <= args.LastLogIndex)) {
				rf.CurrentTerm = args.Term
				rf.VotedFor = args.CandidateID
				reply.Term = args.Term
				reply.VoteGranted = true
				//fmt.Printf("confirm2\n")
			} else {
				rf.CurrentTerm = args.Term
				reply.Term = rf.CurrentTerm
				reply.VoteGranted = false
				//fmt.Printf("reject2\n")
			}
		}
	case Candidate:
		{
			//fmt.Printf("ID %d (candidate) received RequestVote RPC [%d,%d]\n", rf.Me, args.Term, args.CandidateID)
			if rf.CurrentTerm >= args.Term {
				reply.Term = rf.CurrentTerm
				reply.VoteGranted = false
			} else if rf.Log[len(rf.Log)-1].Term < args.LastLogTerm || (rf.Log[len(rf.Log)-1].Term == args.LastLogTerm && len(rf.Log)-1 <= args.LastLogIndex) {
				rf.State = Follower
				rf.CurrentTerm = args.Term
				rf.VotedFor = args.CandidateID
				reply.Term = args.Term
				reply.VoteGranted = true
			} else {
				rf.CurrentTerm = args.Term
				rf.State = Follower
				rf.VotedFor = -1
				reply.Term = rf.CurrentTerm
				reply.VoteGranted = false
			}
		}
	case Leader:
		{
			//fmt.Printf("ID %d (leader) received RequestVote RPC [%d,%d]\n", rf.Me, args.Term, args.CandidateID)
			if rf.CurrentTerm >= args.Term {
				reply.Term = rf.CurrentTerm
				reply.VoteGranted = false
			} else {
				rf.State = Follower
				rf.CurrentTerm = args.Term
				rf.VotedFor = args.CandidateID
				reply.Term = args.Term
				reply.VoteGranted = true
			}
		}
	}
}

func confirmAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply, size int) {
	fmt.Printf("confirmAppendEntries: [%d, src:%d, term %d,PrevLogIndex %d,PrevLogTerm %d, Entries: %v] Log size now: %d\n", server, args.LeaderID, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.Entries, size)
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.Success = false
		//fmt.Printf("ID %d reject1 AppendEntries:[%d, src:%d, term %d,PrevLogIndex %d,PrevLogTerm %d, Entries: %v], reply term %d\n", rf.Me, rf.Me, args.LeaderID, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.Entries, reply.Term)
	} else if len(rf.Log)-1 < args.PrevLogIndex || rf.Log[args.PrevLogIndex].Term != args.PrevLogTerm {
		rf.CurrentTerm = args.Term
		rf.HeartBeatSignal = true
		rf.State = Follower
		reply.Term = rf.CurrentTerm
		reply.Success = false
		//fmt.Printf("ID %d reject2 AppendEntries:[%d, src:%d, term %d,PrevLogIndex %d,PrevLogTerm %d, Entries: %v], reply term %d\n", rf.Me, rf.Me, args.LeaderID, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.Entries, reply.Term)
	} else {
		rf.CurrentTerm = args.Term
		rf.HeartBeatSignal = true
		rf.State = Follower
		rf.Log = append(rf.Log[0:args.PrevLogIndex+1], args.Entries[:]...)

		if rf.CommitIndex < args.LeaderCommit {
			rf.CommitIndex = min(args.LeaderCommit, len(rf.Log)-1)
		}
		rf.reply()
		reply.Term = rf.CurrentTerm
		reply.Success = true
		//confirmAppendEntries(rf.Me, args, reply, len(rf.Log))
	}

}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.Peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.Peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		//fmt.Printf("ID %d send to ID %d, term %d, failed1 in term %d.\n", rf.Me, server, rf.CurrentTerm, rf.CurrentTerm)
		return ok
	}
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	if rf.CurrentTerm > args.Term {
		return ok
	}
	if rf.CurrentTerm < reply.Term {
		//fmt.Printf("ID %d send to ID %d, failed2 in term %d.\n", rf.Me, server, rf.CurrentTerm)
		rf.State = Follower
		rf.CurrentTerm = reply.Term
		rf.VotedFor = -1
		rf.VoteCount = 0
	} else if rf.CurrentTerm == reply.Term && rf.State == Candidate && reply.VoteGranted {
		//fmt.Printf("ID %d send to ID %d, succeed in term %d.\n", rf.Me, server, rf.CurrentTerm)
		rf.VoteCount += 1
		if rf.VoteCount > len(rf.Peers)/2 && rf.VoteCount != 1 {
			rf.State = Leader
			//fmt.Printf("ID %d is leader now in Term %d, Log %v\n", rf.Me, rf.CurrentTerm, rf.Log)
			for i := 0; i < len(rf.Peers); i++ {
				rf.NextIndex[i] = len(rf.Log)
				rf.MatchIndex[i] = 0
				//fmt.Printf("update NextIndex[%d]->%d\n", i, len(rf.Log))
			}
			rf.VotedFor = -1
			rf.VoteCount = 0
			rf.Mu.Unlock()
			rf.sendHeartBeatToAll()
			rf.Mu.Lock()
		}
	} else {
		//fmt.Printf("ID %d send to ID %d, failed3 in term %d.\n", rf.Me, server, rf.CurrentTerm)
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.Peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		return ok
	}
	rf.Mu.Lock()
	if rf.CurrentTerm < reply.Term {
		rf.State = Follower
		//fmt.Printf("Change to follower\n")
		rf.CurrentTerm = reply.Term
		rf.VotedFor = -1
		rf.VoteCount = 0
		rf.Mu.Unlock()
		return ok
	}
	if rf.CurrentTerm > args.Term {
		rf.Mu.Unlock()
		return ok
	}
	if reply.Success {
		rf.NextIndex[server] = rf.NextIndex[server] + len(args.Entries)
		rf.MatchIndex[server] = rf.NextIndex[server] - 1
		//fmt.Printf("ID %d sendAppendEntries(PrevLogIndex %d) to ID %d, %v.\nUpdate NextIndex[%d]:%d, MatchIndex[%d]: %d \n", rf.Me, args.PrevLogIndex, server, reply.Success, server, rf.NextIndex[server], server, rf.MatchIndex[server])
		rf.commitLogs()
		//rf.reply()
		//fmt.Printf("····································\n")
		rf.Mu.Unlock()
		return ok
	} else {
		//fmt.Printf("[Term %d]ID %d sendAppendEntries[%d %d %d] to ID %d, %v.\n Reply:[%d,%v] \n", rf.CurrentTerm, rf.Me, args.Term, args.LeaderID, args.PrevLogIndex, server, reply.Success, reply.Term, reply.Success)
		rf.NextIndex[server] -= 1
		args.Term = rf.CurrentTerm
		args.LeaderID = rf.Me
		args.PrevLogIndex = rf.NextIndex[server] - 1
		args.PrevLogTerm = rf.Log[args.PrevLogIndex].Term
		args.Entries = append(args.Entries[0:0], rf.Log[rf.NextIndex[server]:]...)
		args.LeaderCommit = rf.CommitIndex
		reply.Term = rf.CurrentTerm
		reply.Success = false
		rf.Mu.Unlock()
		//reportApplyEntries(server, args)
		go rf.sendAppendEntries(server, args, reply)
		return ok
	}
}

func (rf *Raft) sendRequestVoteToAll() {
	for i := 0; i < len(rf.Peers); i++ {
		if rf.State == Leader || rf.State == Follower {
			break
		}
		if i != rf.Me {
			rf.Mu.Lock()
			var args RequestVoteArgs
			var reply RequestVoteReply
			args.Term = rf.CurrentTerm
			args.CandidateID = rf.Me
			args.LastLogIndex = len(rf.Log) - 1
			args.LastLogTerm = rf.Log[args.LastLogIndex].Term
			reply.Term = rf.CurrentTerm
			reply.VoteGranted = false
			rf.Mu.Unlock()
			//fmt.Printf("ID %d (%d) send to ID %d, term %d.\n", rf.Me, rf.State, i, rf.CurrentTerm)
			go rf.sendRequestVote(i, args, &reply)
		}
	}
}

func (rf *Raft) sendHeartBeatToAll() {
	for i := 0; i < len(rf.Peers); i++ {
		if i != rf.Me {
			rf.Mu.Lock()
			if rf.State != Leader {
				rf.Mu.Unlock()
				return
			}
			var args AppendEntriesArgs
			var reply AppendEntriesReply
			args.Term = rf.CurrentTerm
			args.LeaderID = rf.Me
			args.PrevLogIndex = rf.NextIndex[i] - 1
			args.PrevLogTerm = rf.Log[args.PrevLogIndex].Term
			args.Entries = args.Entries[0:0]
			args.LeaderCommit = rf.CommitIndex
			reply.Term = rf.CurrentTerm
			reply.Success = false
			rf.Mu.Unlock()
			//reportApplyEntries(i, args)
			go rf.sendAppendEntries(i, args, &reply)
		}
	}
}

func reportApplyEntries(server int, args AppendEntriesArgs) {
	fmt.Printf("AppendEntry:[%d to %d, Term %d, PrevLogIndex %d,PrevLogTerm %d,Entries %v,LeaderCommit %d]\n", args.LeaderID, server, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.Entries, args.LeaderCommit)

}

func (rf *Raft) sendApplyEntriesToAll() {
	for i := 0; i < len(rf.Peers); i++ {
		if i != rf.Me && rf.NextIndex[i] <= len(rf.Log)-1 {
			rf.Mu.Lock()
			if rf.State != Leader {
				rf.Mu.Unlock()
				return
			}
			var args AppendEntriesArgs
			var reply AppendEntriesReply
			args.Term = rf.CurrentTerm
			args.LeaderID = rf.Me
			args.PrevLogIndex = rf.NextIndex[i] - 1
			if args.PrevLogIndex >= 0 {
				args.PrevLogTerm = rf.Log[args.PrevLogIndex].Term
			} else {
				args.PrevLogTerm = 0
			}
			args.Entries = append(args.Entries[0:0], rf.Log[rf.NextIndex[i]:]...)
			args.LeaderCommit = rf.CommitIndex
			reply.Term = rf.CurrentTerm
			reply.Success = false
			rf.Mu.Unlock()
			//reportApplyEntries(i, args)
			go rf.sendAppendEntries(i, args, &reply)
		}
	}
}

func (rf *Raft) commitLogs() {
	//fmt.Printf("Leader %d starts to commit: %d , %d\n", rf.Me,len(rf.Log), rf.CommitIndex)
	for i := len(rf.Log) - 1; i > rf.CommitIndex; i-- {
		if rf.Log[i].Term == rf.CurrentTerm {
			ctr := 1
			for j := 0; j < len(rf.Peers); j++ {
				//fmt.Printf("rf.MatchIndex[%d]: %d\n",j,rf.MatchIndex[j])
				if j != rf.Me && rf.MatchIndex[j] >= i {
					ctr += 1
				}
			}
			//fmt.Printf("%v ctr=%d\n", rf.Log[i], ctr)
			if ctr > len(rf.Peers)/2 {
				rf.CommitIndex = i
				return
			}
		}
	}
	//fmt.Printf("ID %d: CommitIndex: %d,LastApplied:%d\n", rf.Me,rf.CommitIndex, rf.LastApplied)
	rf.reply()
}

func (rf *Raft) reply() {
	if rf.CommitIndex < 0 {
		return
	}
	for i := rf.LastApplied + 1; i <= rf.CommitIndex; i += 1 {
		var applyMsg ApplyMsg
		applyMsg.Index = i

		applyMsg.Command = rf.Log[i].Command
		rf.ApplyCh <- applyMsg
	}
	rf.LastApplied = rf.CommitIndex
}

//
// background goroutine
//
func (rf *Raft) loop() {
	for {
		rf.Mu.Lock()
		switch state := rf.State; state {
		case Follower:
			{
				//fmt.Printf("ID %v, follower, Term %v\n", rf.Me, rf.CurrentTerm)
				rf.Mu.Unlock()
				timer := time.NewTimer(time.Duration(rand.Intn(150)+150) * time.Millisecond)
				<-timer.C
				rf.Mu.Lock()
				if !rf.HeartBeatSignal && rf.State == Follower {
					rf.HeartBeatSignal = false
					//fmt.Printf("%d changes to candidate.\n", rf.Me)
					rf.State = Candidate
					rf.CurrentTerm += 1
					rf.VotedFor = rf.Me
					rf.VoteCount = 1
					rf.Mu.Unlock()
					rf.sendRequestVoteToAll()
				} else {
					rf.HeartBeatSignal = false
					rf.Mu.Unlock()
				}

			}
		case Candidate:
			{
				rf.Mu.Unlock()

				//fmt.Printf("ID %v, candidate,VoteCount %d, Term %v\n", rf.Me, rf.VoteCount, rf.CurrentTerm)
				timer := time.NewTimer(time.Duration(rand.Intn(150)+150) * time.Millisecond)
				<-timer.C

				rf.Mu.Lock()
				if rf.State == Candidate {
					rf.State = Candidate
					rf.CurrentTerm += 1
					rf.VotedFor = rf.Me
					rf.VoteCount = 1
					rf.Mu.Unlock()
					rf.sendRequestVoteToAll()
				} else {
					rf.Mu.Unlock()
				}

			}
		case Leader:
			{
				rf.Mu.Unlock()
				//var new_log Logs
				//fmt.Printf("ID %v, leader, Term %v\n", rf.Me, rf.CurrentTerm)
				timer := time.NewTimer(time.Duration(10) * time.Millisecond)
				<-timer.C
				rf.sendHeartBeatToAll()
				rf.Mu.Lock()
				if rf.State == Leader {
					go rf.commitLogs()
					go rf.sendApplyEntriesToAll()
					go rf.reply()
				}
				rf.Mu.Unlock()
			}
		}
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	index := -1
	Term := rf.CurrentTerm
	isLeader := (rf.State == Leader)

	if isLeader {
		entry := Logs{command, Term}
		rf.Log = append(rf.Log, entry)
		index = len(rf.Log) - 1
	}

	////fmt.Printf("command %d send to ID %d, %v in Term %d\n", index, rf.Me, isLeader, rf.CurrentTerm)

	return index, Term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in Peers[]. this
// server's port is Peers[Me]. all the servers' Peers[] arrays
// have the same order. Persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(Peers []*labrpc.ClientEnd, Me int,
	Persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.Peers = Peers
	rf.Persister = Persister
	rf.Me = Me

	// Your initialization code here.
	rf.ApplyCh = applyCh
	rf.HeartBeatSignal = false
	rf.State = Follower

	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.VoteCount = 0
	rf.Log = []Logs{Logs{nil, 0}}
	rf.CommitIndex = 0
	rf.LastApplied = 0
	rf.NextIndex = make([]int, len(Peers))
	rf.MatchIndex = make([]int, len(Peers))

	for i := 0; i < len(Peers); i++ {
		rf.NextIndex[i] = len(rf.Log)
		rf.MatchIndex[i] = -1
	}

	// initialize from state persisted before a crash
	rf.readPersist(Persister.ReadRaftState())

	go rf.loop()

	return rf
}

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

import "sync"
import "github.com/cmu440/labrpc"
import "math/rand"
import "time"

type Status int

const (
	Leader Status = iota + 1
	Candidate
	Follower
)

const HeartbeatInterval = 100

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
// Information about each log entry.
//
type Entry struct {
	Term    int         // the term in which this entry was created
	Command interface{} // command for the state machine
	Index   int         // position in the log
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int // candidate's term
	CandidateId  int // candidate which requestes vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

//
// AppendEntries RPC arguments structure. Field names start with capital letters.
//
type AppendEntriesArgs struct {
	Term         int      // leader's term
	LeaderId     int      // used for follower to redirect clients. Seems no redirection in Start()
	PrevLogIndex int      // index of log entry immediately preceding new ones
	PrevLogTerm  int      // term of prevLogIndex entry
	Entries      []*Entry // log entries to store
	LeaderCommit int      // leader's commitIndex
}

//
// AppendEntries RPC reply structure. Field names start with capital letters.
//
type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu    sync.Mutex          // Lock to protect shared access to this peer's state
	peers []*labrpc.ClientEnd // RPC end points of all peers
	me    int                 // this peer's index into peers[]

	// Your data here (3A, 3B).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	Status           Status        // whether the server believes it is the leader, overlap with votedFor?
	currentTerm      int           // lastest term server has seen
	votedFor         int           // candidateId that received vote in current term, leader?
	logs             []*Entry      // log entries, log index starts from 1
	appendEntry      chan int      // receive AppendEntry request
	requestVote      chan int      // receive RequestVote request
	electionTimeout  time.Duration // election timeout
	heartbeatTimeout time.Duration // heartbeat timeout
	voteCount        int           // count the number of granted votes
	promiseIndex     int

	// volatile state on all servers
	commitIndex int // index of highest log entry known to be committed
	applyCh     chan ApplyMsg

	// volatile state on leaders, reinitialized after election
	nextIndex  map[int]int
	matchIndex map[int]int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	term = rf.currentTerm
	if rf.Status == Leader {
		isleader = true
	} else {
		isleader = false
	}
	rf.mu.Unlock()

	return term, isleader
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	DPrintf("RequestVote:: current term: %d, term: %d, votedFor: %d, current server: %d, candidateId %d, last log index %d, last log term %d.\n",
		rf.currentTerm, args.Term, rf.votedFor, rf.me, args.CandidateId, args.LastLogIndex, args.LastLogTerm)
	if args.Term < rf.currentTerm {
		DPrintf("RequestVote:: Receiver implementation 1.\n")
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.mu.Unlock()
		return
	}
	uptodate := rf.UptodateChecker(args)

	if uptodate {
		DPrintf("RequestVote:: current server: %d vote granted to candidate: %d.\n", rf.me, args.CandidateId)
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		rf.Status = Follower
	} else {
		DPrintf("RequestVote:: not up-to-date enough.\n")
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}

	rf.mu.Unlock()
}

func (rf *Raft) UptodateChecker(args *RequestVoteArgs) bool {
	length := len(rf.logs)
	DPrintf("UptodateChecker:: from server %d, current server %d length: %d .\n", args.CandidateId, rf.me, length)
	// current log is empty

	if length == 0 {
		return true
	} else {
		lastEntry := rf.logs[length-1]
		DPrintf("UptodateChecker:: last entry term%d, index%d, args last log term%d, index%d.\n",
			lastEntry.Term, lastEntry.Index, args.LastLogTerm, args.LastLogIndex)
		if lastEntry.Term > args.LastLogTerm ||
			(lastEntry.Term == args.LastLogTerm && lastEntry.Index > args.LastLogIndex) {
			return false
		} else {
			return true
		}
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
	if ok {
		if reply.VoteGranted {
			rf.mu.Lock()
			rf.voteCount++

			if rf.Status == Candidate && rf.voteCount > len(rf.peers)/2 {
				DPrintf("sendRequestVote:: server %d becomes leader.\n", rf.me)
				rf.Status = Leader
				rf.nextIndex = make(map[int]int)
				rf.matchIndex = make(map[int]int)
				rf.promiseIndex = len(rf.logs) + 1
				length := len(rf.logs)
				for index, _ := range rf.peers {
					rf.nextIndex[index] = length + 1 // initialized to leader last log index + 1
					rf.matchIndex[index] = 0         // initialized to 0
					DPrintf("sendRequestVote:: server %d original next index %d, match index %d.\n",
						index, rf.nextIndex[index], rf.matchIndex[index])
				}
				go rf.sendHeartbeat()
			}
			rf.mu.Unlock()

		} else {
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				DPrintf("sendRequestVote:: server: %d with Term: %d receives higher Term: %d.\n", rf.me, rf.currentTerm, args.Term)
				rf.currentTerm = reply.Term
				rf.Status = Follower
			}
			rf.mu.Unlock()
		}
	} else {
		ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
	}

	return ok
}

func (rf *Raft) sendHeartbeat() {
	rf.heartBeat()
	tick := time.Tick(rf.heartbeatTimeout)

	for {
		select {
		case <-tick:
			rf.heartBeat()
		}
	}
}

func (rf *Raft) heartBeat() {
	rf.mu.Lock()
	if rf.Status == Leader {
		for index, _ := range rf.peers {
			if index != rf.me {
				args := &AppendEntriesArgs{}
				args.Term = rf.currentTerm
				args.LeaderCommit = rf.commitIndex
				args.LeaderId = rf.me
				args.PrevLogIndex = rf.nextIndex[index] - 1
				//				args.PrevLogIndex = len(rf.logs)
				if args.PrevLogIndex == 0 {
					args.PrevLogTerm = 0
				} else {
					args.PrevLogTerm = rf.logs[args.PrevLogIndex-1].Term // index attention!!
				}

				if len(rf.logs) >= rf.nextIndex[index] {
					args.Entries = make([]*Entry, 0)
					for i := args.PrevLogIndex; i < len(rf.logs); i++ {
						args.Entries = append(args.Entries, rf.logs[i])
						//						DPrintf("heartBeat:: command %s is added to entries.\n", rf.logs[i].Command)
					}
				}
				reply := &AppendEntriesReply{}
				go rf.sendAppendEntries(index, args, reply)
			}
		}
	}
	rf.mu.Unlock()
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
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	DPrintf("Start:: command comming in: %s.\n", command)
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).
	// if this server isn't the leader, return false
	rf.mu.Lock()
	if rf.Status == Leader {
		index = rf.promiseIndex
		rf.promiseIndex++
		term = rf.currentTerm

		length := len(rf.logs)
		// leader appends the command to its log as a new entry
		entry := &Entry{}
		entry.Term = rf.currentTerm
		entry.Index = length + 1
		entry.Command = command
		rf.logs = append(rf.logs, entry)

		DPrintf("Start:: command %s, index %d, term %d.\n", command, index, term)
		NPrintf("Start:: command: %s, index: %d, term: %d.\n", command, index, term)
		rf.mu.Unlock()
		go rf.makeConsensus()
	} else {
		isLeader = false
		rf.mu.Unlock()
	}

	return index, term, isLeader
}

func (rf *Raft) makeConsensus() {
	for index, _ := range rf.peers {
		//		DPrintf("makeConsensus:: index %d current %d.\n", index, rf.me)
		rf.mu.Lock()
		if index != rf.me && len(rf.logs) >= rf.nextIndex[index] {
			appendArgs := &AppendEntriesArgs{}
			appendArgs.LeaderId = rf.me
			appendArgs.LeaderCommit = rf.commitIndex
			appendArgs.Term = rf.currentTerm
			// send AppendEntries RPC with log entries starting at nextIndex
			//			appendArgs.PrevLogIndex = len(rf.logs) - 1
			appendArgs.PrevLogIndex = rf.nextIndex[index] - 1
			if appendArgs.PrevLogIndex == 0 {
				appendArgs.PrevLogTerm = 0
			} else {
				appendArgs.PrevLogTerm = rf.logs[appendArgs.PrevLogIndex-1].Term
			}
			for i := appendArgs.PrevLogIndex; i < len(rf.logs); i++ {
				appendArgs.Entries = append(appendArgs.Entries, rf.logs[i])
			}
			rf.mu.Unlock()

			reply := &AppendEntriesReply{}
			DPrintf("makeConsensus:: server %d sends AppendEntries with %d command to server %d.\n", rf.me, len(appendArgs.Entries), index)
			go rf.sendAppendEntries(index, appendArgs, reply)
		} else {
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	rf.mu.Lock()
	if rf.Status != Leader {
		rf.mu.Unlock()
		return false
	}
	//	DPrintf("sendAppendEntries:: original reply: %t.\n", reply.Success)
	//	if len(args.Entries) > 0 {
	//		DPrintf("sendAppendEntries:: current server%d sends AppendEntries to server%d with %d entries, commit %d, index %d, prevTerm %d, term %d.\n",
	//			rf.me, server, len(args.Entries), args.LeaderCommit, args.PrevLogIndex, args.PrevLogTerm, args.Term)
	//	}
	rf.mu.Unlock()

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	// RPC call returns successfully
	if ok {
		// append entries successfully
		if reply.Success {
			// heartbeat --> ignore
			if len(args.Entries) == 0 {
				return ok
			}
			// successful: update nextIndex and matchIndex for follower
			nextLogIndex := args.PrevLogIndex + len(args.Entries) + 1
			rf.mu.Lock()
			//			nextnext := rf.nextIndex[server]
			rf.nextIndex[server] = nextLogIndex
			//			DPrintf("sendAppendEntries:: update server%d's next index from %d to %d.\n", server, nextnext, rf.nextIndex[server])
			//			match := rf.matchIndex[server]
			rf.matchIndex[server] = nextLogIndex - 1
			//			DPrintf("sendAppendEntries:: update server%d's matched index from %d to %d.\n", server, match, rf.matchIndex[server])

			original := rf.commitIndex
			rf.updateCommitIndex(original)
			commit := rf.commitIndex
			DPrintf("sendAppendEntries:: original commit %d, current commit %d.\n", original, commit)
			if commit > original {
				//			if commit >= original {
				var startIndex int
				if original < args.PrevLogIndex {
					startIndex = original
				} else {
					startIndex = args.PrevLogIndex
				}
				startIndex = 0
				//				DPrintf("sendAppendEntries:: original %d, prevLogIndex %d, startIndex %d.\n", original, args.PrevLogIndex, startIndex)
				for i := startIndex; i < commit; i++ {
					//				for i := args.PrevLogIndex; i < commit; i++ {
					applyMsg := &ApplyMsg{}
					applyMsg.Command = rf.logs[i].Command
					applyMsg.Index = rf.logs[i].Index
					rf.applyCh <- *applyMsg
					DPrintf("sendAppendEntries:: leader %d sends ApplyMsg back with command %s.\n", rf.me, applyMsg.Command)
					// applyEntries.Entries = append(applyEntries.Entries, rf.logs[i])
					// DPrintf("sendAppendEntries:: entry %d with command %s and term %d is added to ApplyMsg.\n", i, rf.logs[i].Command, rf.logs[i].Term)
				}
			}
			rf.mu.Unlock()
		} else {
			// follower "server" denied the agreement
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				DPrintf("sendAppendEntries:: server%d with larger term%d than leader%d with term %d.\n", server, reply.Term, rf.me, rf.currentTerm)
				rf.currentTerm = reply.Term
				rf.Status = Follower
				rf.mu.Unlock()
				return ok
			} else {
				if rf.nextIndex[server] > 1 {
					rf.nextIndex[server]--
				}
				//				rf.nextIndex[server]--
				DPrintf("sendAppendEntreis:: decrement server%d's next index to %d.\n", server, rf.nextIndex[server])
				appendArgs := &AppendEntriesArgs{}
				appendArgs.LeaderCommit = rf.commitIndex
				appendArgs.LeaderId = rf.me
				appendArgs.Term = rf.currentTerm
				appendArgs.PrevLogIndex = rf.nextIndex[server] - 1
				if appendArgs.PrevLogIndex == 0 {
					appendArgs.PrevLogTerm = 0
				} else {
					appendArgs.PrevLogTerm = rf.logs[appendArgs.PrevLogIndex-1].Term
				}
				//				args.Entries = make([]*Entry, 0)
				for i := rf.nextIndex[server] - 1; i < len(rf.logs); i++ {
					appendArgs.Entries = append(appendArgs.Entries, rf.logs[i])
				}
				rf.mu.Unlock()
				appendReply := &AppendEntriesReply{}
				go rf.sendAppendEntries(server, appendArgs, appendReply)
			}
		}
	} else {
		//		go rf.sendAppendEntries(server, args, reply)
	}
	return ok
}

func (rf *Raft) updateCommitIndex(original int) {
	if rf.Status != Leader {
		return
	}
	//	original := rf.commitIndex
	//	DPrintf("original: %d, current server %d.\n", original, rf.me)
	// rf.mu.Lock()
	for i := original; i < len(rf.logs); i++ {
		count := 1
		for index, match := range rf.matchIndex {
			//			DPrintf("updateCommitIndex:: index %d, match %d.\n", index, match)
			//			if index != rf.me && match >= i + 1 {
			if index != rf.me && match >= i+1 && rf.logs[i].Term == rf.currentTerm {
				//				DPrintf("count updated.\n")
				count++
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = i + 1
			//			DPrintf("updateCommitIndex:: current commit index: %d.\n", rf.commitIndex)
		}
	}
	// rf.mu.Unlock()
	DPrintf("updateCommitIndex:: server%d original index: %d. final commit index: %d.\n", rf.me, original, rf.commitIndex)
}

// AppendEntries handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// implementation 1
	//	if len(args.Entries) > 0 {
	//		DPrintf("AppendEntries:: server%d receives AppendEntries from server%d with %d entries.\n",
	//			rf.me, args.LeaderId, len(args.Entries))
	//	}
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		DPrintf("AppendEntries:: Receiver implementation 1, server%d args.Term: %d, current term: %d, reply %t.\n",
			rf.me, args.Term, rf.currentTerm, reply.Success)
		//		NPrintf("AppendEntries:: Receiver implementation 1, server%d args.Term: %d, current term: %d, reply %t.\n",
		//			rf.me, args.Term, rf.currentTerm, reply.Success)
		rf.mu.Unlock()
		return
	}

	// implementation 2
	prevLogCheck := rf.prevLogChecker(args)
	//	DPrintf("AppendEntries:: current server %d prevLogCheck: %t, entry length %d.\n", rf.me, prevLogCheck, len(args.Entries))
	//	if len(args.Entries) > 0 {
	//		DPrintf("AppendEntries:: current server %d prevLogCheck: %t.\n", rf.me, prevLogCheck)
	//	}
	if prevLogCheck {
		rf.appendEntry <- 1
		rf.Status = Follower
		rf.currentTerm = args.Term

		reply.Success = true
		reply.Term = rf.currentTerm

		// implementation 3
		length := len(rf.logs)
		//		NPrintf("AppendEntries:: server%d, leader %d, length: %d, PrevLogIndex: %d.\n", rf.me, args.LeaderId, length, args.PrevLogIndex)
		var reserve int
		if length < args.PrevLogIndex {
			reserve = length
		} else {
			reserve = args.PrevLogIndex
		}
		//		NPrintf("AppendEntries:: reserve: %d.\n", reserve)
		rf.logs = rf.logs[:reserve]
		// implementation 4
		for i := 0; i < len(args.Entries); i++ {
			rf.logs = append(rf.logs, args.Entries[i])
			DPrintf("AppendEntries:: server %d appends command %s into log.\n",
				rf.me, args.Entries[i].Command)
		}
		DPrintf("AppendEntries: leader commit %d, current commit %d.\n", args.LeaderCommit, rf.commitIndex)

		if args.LeaderCommit > rf.commitIndex {
			origCommit := rf.commitIndex
			if args.LeaderCommit > len(rf.logs) {
				rf.commitIndex = len(rf.logs)
			} else {
				rf.commitIndex = args.LeaderCommit
			}
			DPrintf("AppendEntries:: original commit %d, current commit %d.\n", origCommit, rf.commitIndex)
			for i := origCommit; i < rf.commitIndex; i++ {
				applyMsg := &ApplyMsg{}
				applyMsg.Index = i + 1
				applyMsg.Command = rf.logs[i].Command
				rf.applyCh <- *applyMsg
				DPrintf("AppendEntries:: server %d send ApplyMsg back with command %s.\n", rf.me, applyMsg.Command)
			}
			//			DPrintf("AppendEntries:: leader commit: %d, length: %d. server%d updated commit: %d.\n", args.LeaderCommit, len(rf.logs), rf.me, rf.commitIndex)
		}
		rf.mu.Unlock()
	} else {
		reply.Success = false
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
	}
}

func (rf *Raft) prevLogChecker(args *AppendEntriesArgs) bool {
	length := len(rf.logs)
	// if len(args.Entries) > 0 {
	DPrintf("prevLogCheck:: current server%d log length %d, prevLogIndex: %d, prevLogTerm: %d, %d commands\n",
		rf.me, length, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries))
	// }

	// condition 1: current log is smaller than leader's log
	if length < args.PrevLogIndex {
		//		DPrintf("prevLogCheck:: length of current log is smaller than length of leader's log.\n")
		return false
	}

	// condition 2: current log is equal to or greater than leader's log
	if args.PrevLogIndex == 0 {
		//		DPrintf("prevLogCheck:: length of leader's log is 0, return true.\n")
		return true
	} else {
		entry := rf.logs[args.PrevLogIndex-1]
		//		DPrintf("prevLogCheck:: current server %d last entry index %d, term %d, command %s.\n", rf.me, entry.Index, entry.Term, entry.Command)
		if args.PrevLogTerm != entry.Term {
			return false
		} else {
			return true
		}
	}
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf = &Raft{}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int, applyCh chan ApplyMsg) *Raft {
	DPrintf("Make is called. Length of peers: %d, me: %d.\n", len(peers), me)
	rf := &Raft{}
	rf.peers = peers
	rf.me = me

	// Your initialization code here (3A, 3B).
	rf.mu = *new(sync.Mutex)
	rf.mu.Lock()
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]*Entry, 0)
	rf.applyCh = applyCh
	rf.appendEntry = make(chan int, 2)
	rf.requestVote = make(chan int, 2)
	rf.commitIndex = 0
	rf.Status = Follower
	rf.heartbeatTimeout = time.Duration(HeartbeatInterval) * time.Millisecond
	rf.mu.Unlock()

	go rf.MainRoutine()

	return rf
}

func (rf *Raft) MainRoutine() {
	rf.electionTimeout = time.Duration(rand.Intn(300-150)+250) * time.Millisecond
	//	rf.electionTimeout = time.Duration(rand.Intn(300-150)+150) * time.Millisecond
	DPrintf("MainRoutine:: current server: %d, election timeout is: %d.\n", rf.me, rf.electionTimeout)
	for {
		select {
		case <-rf.appendEntry: // leader exists, reset the election timeout
			//				DPrintf("MainRoutine:: server%d's append entry triggered, reset election timeout.\n", rf.me)
			rf.electionTimeout = time.Duration(rand.Intn(300-150)+250) * time.Millisecond
		case <-rf.requestVote:
			//				DPrintf("MainRoutine:: server%d's request vote triggered, reset election timeout.\n", rf.me)
			rf.electionTimeout = time.Duration(rand.Intn(300-150)+250) * time.Millisecond
		case <-time.After(rf.electionTimeout):
			rf.mu.Lock()
			DPrintf("MainRoutine:: current server: %d, isLeader: %t.\n", rf.me, rf.Status == Leader)
			if rf.Status != Leader {
				DPrintf("MainRoutine:: start the election.\n")
				// rf.mu.Lock()
				rf.currentTerm = rf.currentTerm + 1
				rf.Status = Candidate
				rf.voteCount = 1
				for index, _ := range rf.peers {
					if index != rf.me {
						args := &RequestVoteArgs{}
						args.Term = rf.currentTerm
						args.CandidateId = rf.me
						length := len(rf.logs) // log index starts from 1, accessing logs requires pay attention to index
						args.LastLogIndex = length
						if length == 0 {
							args.LastLogTerm = 0
						} else {
							args.LastLogTerm = rf.logs[args.LastLogIndex-1].Term
						}
						reply := &RequestVoteReply{}
						go rf.sendRequestVote(index, args, reply)
					}
				}
				// rf.mu.Unlock()
			}
			rf.mu.Unlock()
		}
	}
}

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
import "labrpc"
import "time"
import "math/rand"
// import "fmt"

// import "bytes"
// import "labgob"

const LEADER = 0;
const FOLLOWER = 1; 
const CANDIDATE = 2;  

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]


	Log       []AppendEntries
	Status    int   //0 if you are a leader, 1 if you are follower, 2 if you are candidate (see consts)
	VotedFor  int   //who you voted for. null if none.
	VotedForTerm int //term that you last voted in
	CurrentTerm int 
	HeartBeatReceived bool // If false when the heartbeatlistener times out, start election
	NumOfVotes int
	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

//if command is nil, it's a hearbeat, else it's a log entry
type AppendEntries struct{
	Command interface{}
	Term int
}

type AppendReply struct {

}

func(rf *Raft) GetLastLogIndex() (int){
	if len(rf.Log) == 0{
		return 0
	} else {
		index := len(rf.Log) - 1 
		return index
	}
}
func(rf *Raft) GetLastLogTerm() (int){
	if len(rf.Log) == 0 {
		//if log is empty, our last log term is 0
		return 0
	} else {
		index := len(rf.Log) - 1 
		term := rf.Log[index].Term
		return term
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	return rf.CurrentTerm, rf.Status == LEADER
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term int
	CandidateID int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (3A).
	Vote bool
	Term int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	// Check that term >= CurrentTerm
	// Check if we've voted for someone this term
	// fmt.Println(rf.me,"has received a request from",args.CandidateID)
	rf.mu.Lock()
	// Cannot get a heartbeat... will have to think about this
	if args.Term >= rf.CurrentTerm && args.Term > rf.VotedForTerm {
		// Check if candidate's log is as up to date as ours
		if rf.GetLastLogIndex() <= args.LastLogIndex {
			// Check then it has as many items
			if rf.GetLastLogTerm() <= args.LastLogTerm {
				// Set votedforterm to be this term of the request
				// fmt.Println(rf.me,"is voting for",args.CandidateID)
				rf.VotedForTerm = args.Term
				rf.VotedFor = args.CandidateID
				reply.Vote = true
				reply.Term = args.Term
			}
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) HeartBeatReceiver(args *AppendEntries, reply *AppendReply) {
	// Your code here 3A
	// Check that term of heartbeat is >= own term
	// fmt.Println("Received Heart Beat")
	rf.mu.Lock()
	if args.Term >= rf.CurrentTerm {
		// Change status to follower
		rf.Status = FOLLOWER
		// Change hearbeatreceived to true
		rf.HeartBeatReceived = true
		// Change term to heartbeat's
		rf.CurrentTerm = args.Term
	}
	rf.mu.Unlock()
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
	if reply.Vote {
		rf.mu.Lock()
		rf.NumOfVotes += 1
		rf.mu.Unlock()
	}
	return ok
}


func (rf *Raft) sendHeartBeat(server int, args *AppendEntries, reply *AppendReply) bool {
	ok := rf.peers[server].Call("Raft.HeartBeatReceiver", args, reply)
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

	// Your code here (3B).

	return index, term, isLeader
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

	rf.Log = make([]AppendEntries, 0)
	rf.Status = FOLLOWER
	rf.VotedFor = -1
	rf.VotedForTerm = -1
	rf.CurrentTerm = 0
	rf.HeartBeatReceived = false
	rf.NumOfVotes = 0
	go func() {
		for {
			switch rf.Status {
			case FOLLOWER:
				// Set receivedheartbeat to false
				rf.mu.Lock()
				rf.HeartBeatReceived = false
				rf.mu.Unlock()
				// Wait until timeout
				time.Sleep(time.Duration(300 + rand.Intn(200)) * time.Millisecond)
				// Check if we've received a heartbeat
				// If yes, whoop dee doo
				// If no, become candidate
				rf.mu.Lock()
				if !rf.HeartBeatReceived {
					rf.Status = CANDIDATE
				}
				rf.mu.Unlock()
			case CANDIDATE:
				// Election, oh dear
				// Increment term
				rf.mu.Lock()
				rf.CurrentTerm += 1
				// vote for self
				rf.NumOfVotes = 1
				rf.VotedFor = rf.me
				rf.VotedForTerm = rf.CurrentTerm
				rf.mu.Unlock()
				// Start election timer
				timeout := make(chan bool)
				go func() {
					time.Sleep(time.Duration(300 + rand.Intn(100)) * time.Millisecond)
					timeout <- false //timeout
				}()
				// Make our requestvotearg
				
				// Send requestvote to everyone
				for i:= 0; i < len(rf.peers); i++ {
					if i != rf.me {
						Votearg := RequestVoteArgs{
							Term: rf.CurrentTerm,
							CandidateID: rf.me,
							LastLogIndex: rf.GetLastLogIndex(),
							LastLogTerm: rf.GetLastLogTerm(),
						}
						VoteReply := RequestVoteReply{Vote: false, Term: rf.CurrentTerm}
						go rf.sendRequestVote(i, &Votearg, &VoteReply)
					}
				}
				Loop:
					for {
						select {
						case <- timeout:
							//fmt.Println("Timeout")
							break Loop
						default:
							rf.mu.Lock()
							if rf.Status == FOLLOWER { // If we received a heartbeat
								rf.mu.Unlock()
								break Loop
							}
							if rf.NumOfVotes * 2 > len(rf.peers) {
								// What if we pause here?
								// IF someone else started a new election and we voted for them, our term is oudated
								// NO ONE could have become a leader in this current term
								// THEREFORE it is safe make ourself the Supreme Chancellor of the Republic
								rf.Status = LEADER
								//fmt.Println("Just assigned",rf.me,"to LEADER")
								rf.mu.Unlock()
								break Loop
							}
							rf.mu.Unlock()
						}
					}
				// Either:
					// Timeout
					// Check if became follower (maybe)
					// Get majority and become leader
			case LEADER:
				// Send heartbeats to all followers
				//fmt.Println("I'm the leader!")
				for i := 0; i < len(rf.peers); i++ {
					if i != rf.me {
						heartbeat := AppendEntries{Command: nil, Term: rf.CurrentTerm}
						reply := AppendReply{}
						go rf.sendHeartBeat(i, &heartbeat, &reply)
					}
				}
				time.Sleep(150 * time.Millisecond)
			}
			// Set receivedheratbeat to false here?
		}
		//fmt.Println("Should never reach end of infinite loop")
	}()

	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

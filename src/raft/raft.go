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
//import "math/rand"
//import "labgob"


const leader = 0;
const follower = 1; 
const candidate = 2;  


// import "bytes"

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

//One of these raft structs for each server
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
	HeartbeatReceived bool // If false when the heartbeatlistener times out, start election
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

//if command is null, it's a hearbeat, else it's a log entry
type AppendEntries struct{
	Command interface{}
	Term int
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
	var term int = rf.CurrentTerm
	return term, rf.Status==leader
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
	VoteChannel chan bool  //send your votes down this channel

}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	//send your vote response (true/false) down the reply channel
	//in this func, rf is the server who is voting
	if args.Term > rf.VotedForTerm{
		rf.VotedForTerm = args.Term
		rf.VotedFor = -1
	}
	if (rf.VotedFor == -1 || rf.VotedFor == args.CandidateID) && args.Term == rf.VotedForTerm {
		//if chandidates log is at-least as up to date as reciever's log
		if (args.LastLogIndex >= rf.GetLastLogIndex()) && (args.LastLogTerm >= rf.GetLastLogTerm()){
			reply.VoteChannel <- true
			rf.VotedFor = args.CandidateID
			rf.VotedForTerm = args.Term
		}
	}
	//else, do nothing!
	
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
func (rf *Raft) SendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}




//our heartbeat RPC handler should be similar to this?
//RPC handler is listening for heartbeats. we have to make that. it is a method of the raft struct

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

//RPC handler for listening to heartbeats
// Is called when a new heartbeat is received
// Contains code for heartbeat listener as well as elections
func (rf *Raft) HeartbeatListener(){
	time.Sleep(500 * time.Millisecond)
	// Sleep for duration of listen (150ms or something)
	// On wakeup, check value of rf.heartBeatReceived
	// If true:
	if rf.HeartbeatReceived {
		rf.HeartbeatReceived = false
	} else { //initiate election
		rf.Status = candidate
		for rf.Status == candidate { // only exits when become a follower or leader
			rf.CurrentTerm += 1
			votearg := RequestVoteArgs{Term:rf.CurrentTerm, 
										CandidateID: rf.me, 
										LastLogIndex: rf.GetLastLogIndex(), 
										LastLogTerm: rf.GetLastLogTerm()}
			myChannel := make(chan bool)
			voteReply := RequestVoteReply{VoteChannel: myChannel}
			//"votes received from a majority of servers become leader"
			for i:= 0; i < len(rf.peers); i++ { //sending voteRequest to all servers
				rf.SendRequestVote(i, &votearg, &voteReply)
			}
			//start the election timer
			 //after the election timer, count votes in your channel
			go func() {
				time.Sleep(300 * time.Millisecond)
				voteReply.VoteChannel <- false //timeout
			}()
			//now count votes. If we receive a majority of trues then we are leader
			voteCount := 0
			for voteCount*2 <= len(rf.peers) {
				vote := <- voteReply.VoteChannel
				if !vote {
					//timeout
					break 
				}
				voteCount += 1; 
			}
			if rf.Status == follower{
				break
			}
			if voteCount*2 > len(rf.peers){
				//we are the leader!
				rf.Status = leader 
				defer rf.SendHeartbeat()
			}
		}
	}
}
//do we need to make a function to send heartbeats? This would be send appendentries
func (rf *Raft) SendHeartbeat(){
	if rf.Status == leader{
		// loop through each peer and send a heartbeat using the Call rpc. Don't need response, don't call self
		heartbeat := AppendEntries{Command: nil, Term: rf.CurrentTerm}
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				rf.peers[i].Call("Raft.HeartbeatReceiver", &heartbeat, nil)
			}
			//if i am sending something to myself. dont wanna do that
		}
	}
	// set timeout
	defer rf.SendHeartbeat()
}

func (rf *Raft) HeartbeatReceiver(heartbeat AppendEntries) {
	if heartbeat.Term >= rf.CurrentTerm{
		rf.HeartbeatReceived = true
		rf.Status = follower
		rf.CurrentTerm = heartbeat.Term
		defer rf.HeartbeatListener() // This should work, as long as the rpc thinks it went well
	}
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

	defer rf.HeartbeatListener()
	// Assign values to objects,
	// Defer heartbeat listener
	//   Must be a method
	// Return Raft

	// Unimplemented:
	// What to do if you are the leader
	// Changing from candidate to follower if received AppendEntries RPC

	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

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
	"sync"
	"labrpc"
    "time"
    "math/rand"
)

const LEADER = 2
const CANDIDATE = 1
const FOLLOWER = 0

const DECREMENT = 0
const INCREMENT = 1
const OTHERFAILURE = 2

// import "bytes"
// import "labgob"

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
	mu 				sync.Mutex          // Lock to protect shared access to this peer's state
	peers 			[]*labrpc.ClientEnd // RPC end points of all peers
	persister 		*Persister          // Object to hold this peer's persisted state
	me 				int                 // this peer's index into peers[]

	status			int
	votes 			int
	currentTerm		int
	currentLeader	int
	votedFor		int
	log 			[]Command
	voteChan		chan Vote
	heartBeatChan 	chan bool
	nextIndex 		[]int
	entryReceived 	[]int
	committedEntry 	int
	nextCommitted 	int
	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

type Vote struct {
	Vote 	bool
	Term 	int
}

type Command struct {
	Command interface{}
	Term 	int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// term := rf.currentTerm
	// isleader := rf.status == LEADER
	// rf.mu.Unlock()
	// Your code here (3A).
	return rf.currentTerm, rf.status == LEADER
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

// Only called when locked
func (rf *Raft) getLastLogIndex() int {
	return len(rf.log) - 1
}
// Only called when locked
func (rf *Raft) getLastLogTerm() int {
	if rf.getLastLogIndex() < 0 {
		return -1
	} else {
		return rf.log[len(rf.log)-1].Term
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	LastLogIndex 	int
	LastLogTerm 	int
	CandidateID 	int
	Term 			int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (3A).
	Term 	int
	Vote 	bool
}

// type HeartBeatArgs struct {
// 	Term 	int
// 	Leader 	int
// }

// type HeartBeatReply struct {
// 	Valid 	bool
// }

type AppendEntryArgs struct {
	LastLogIndex 	int
	LastLogTerm 	int
	Entries 		[]Command
	CommitLevel		int
	Term 			int
	Leader 			int
}

type AppendEntryReply struct {
	Success 	 	int
	Term 			int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock() // Doesn't happen often, can lock for full time
	// Check if term is equal to ours
	if args.Term > rf.currentTerm {
		rf.status = FOLLOWER
	}
	if args.Term > rf.currentTerm || (args.Term == rf.currentTerm && rf.votedFor == -1) {
		rf.currentTerm = args.Term
		if rf.getLastLogTerm() <= args.LastLogTerm {
			if rf.getLastLogIndex() <= args.LastLogIndex || rf.getLastLogTerm() < args.LastLogTerm {
				// Grant vote
				rf.votedFor = args.CandidateID
				reply.Vote = true
				rf.currentTerm = args.Term
				reply.Term = rf.currentTerm
				rf.mu.Unlock()
				return
			}
		}
	}
	reply.Vote = false
	reply.Term = rf.currentTerm
	rf.mu.Unlock()
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	// If we get a term larger than ours, subvert to it
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.currentLeader = args.Leader
	}
	// If someone has a higher commit level, we have to go with it
	if args.Term == rf.currentTerm || args.CommitLevel > rf.committedEntry {
			// Immediately become follower and update term
			rf.status = FOLLOWER
			reply.Term = rf.currentTerm
			// if our log is longer than leader's, cut off everythangggggg
			if len(rf.log) - 1 > args.LastLogIndex {
				rf.log = rf.log[:args.LastLogIndex+1]
			}
			// If our log is the correct length
			if rf.getLastLogIndex() == args.LastLogIndex {
				// And the terms match, we can now:
				//   add anything
				//   update our latest committed entry
				// and then let leader know we're caught up
				if rf.getLastLogTerm() == args.LastLogTerm { // Case of correct
					// Add records
					for len(args.Entries) > 0 {
						rf.addEntry(args.Entries[0].Command,args.Entries[0].Term)
						args.Entries = args.Entries[1:len(args.Entries)]
					}
					// Let leader know we're gucci now
					reply.Success = INCREMENT
					// Set our commit level to either our size or the commit level
					if rf.committedEntry < args.CommitLevel {
						rf.committedEntry = args.CommitLevel
					}
					rf.mu.Unlock()
				} else { // Case of not matching terms, need to step back one
					// Cut it!
					rf.log = rf.log[:len(rf.log)-1]
					// We need to step back one
					reply.Success = DECREMENT
					rf.mu.Unlock()
				}
			} else { // Case of too short
				// Also need to step back one
				reply.Success = DECREMENT
				rf.mu.Unlock()
			}
			// Must unlock before sending things on channels
			// Even if our logs don't match yet, still subvert ourselves
			rf.heartBeatChan <- true
	} else { // This is us rejecting a heartbeat/append entry
		// because it's too old (or commit level is too low)
		reply.Success = OTHERFAILURE
		rf.mu.Unlock()
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
		// Send down the chan, candidate will determine if valid or not
		rf.voteChan <-Vote{Vote:reply.Vote,Term:reply.Term}
	} else {
	}
	return ok
}

// func (rf *Raft) sendHeartBeat(server int, args *HeartBeatArgs, reply *HeartBeatReply) bool {
// 	ok := rf.peers[server].Call("Raft.HeartBeat", args, reply)
// 	return ok
// }

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	rf.mu.Lock()
	if ok {
		if reply.Success == DECREMENT && reply.Term == rf.currentTerm {
			rf.nextIndex[server]--
		} else if reply.Success == INCREMENT && reply.Term == rf.currentTerm {
			rf.entryReceived[server] = args.LastLogIndex + len(args.Entries)
			rf.nextIndex[server] = rf.entryReceived[server]+1
		}
	}
	rf.mu.Unlock()
	return ok
}

// Always locked before entering
func (rf *Raft) addEntry(command interface{},term int) {
	rf.log = append(rf.log,Command{command,term})
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.status != LEADER { // If not leader, reject
		return -1, -1, false
	} // else
	// Add entry to our log
	rf.addEntry(command, rf.currentTerm)
	rf.entryReceived[rf.me] = rf.getLastLogIndex()
	// the leader section of the goroutine in Make will
	// catch the followers up
	return len(rf.log), rf.currentTerm, true
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

	// Your initialization code here (3A, 3B, 3C).
	rf.status = FOLLOWER
	rf.votes = 0
	rf.currentTerm	= 0
	rf.currentLeader = -1
	rf.votedFor = -1
	rf.log = make([]Command, 0)
	rf.nextIndex = make([]int, len(peers))
	rf.entryReceived = make([]int, len(peers))
	rf.committedEntry = -1
	rf.nextCommitted = -1
	rf.voteChan = make(chan Vote)
	rf.heartBeatChan = make(chan bool)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go func() {
		for {
			rf.mu.Lock() // Have to lock here if switching on state
			for rf.committedEntry > rf.nextCommitted { // All people see if they can commit
				// This puts all non-sent commits down the applyCn
				rf.nextCommitted++
				msg := ApplyMsg {
					CommandValid: true,
					Command: rf.log[rf.nextCommitted].Command,
					CommandIndex: rf.nextCommitted+1, // Ours is 0-indexed, tester's is 1-indexed
				}
				applyCh <- msg
			}
			switch rf.status {
				case FOLLOWER:
					rf.mu.Unlock()
					// Waits for event
					select {
						// Either it gets a heartbeat, or it timesout
						case <-time.After(time.Duration(rand.Intn(100) + 600) * time.Millisecond):
							rf.mu.Lock()
							rf.status = CANDIDATE
							rf.mu.Unlock()
						case <-rf.heartBeatChan:
							// Still a follower, do nothing
					}
				case CANDIDATE:
					// Increment term every time we send out votes
					rf.currentTerm += 1
					// i hAvE nO lEaDeR
					rf.currentLeader = -1
					// Set our vote count to 1
					// cause we voted for ourself
					rf.votedFor = rf.me
					rf.votes = 1
					args := RequestVoteArgs{
						LastLogIndex: rf.getLastLogIndex(),
						LastLogTerm: rf.getLastLogTerm(),
						CandidateID: rf.me,
						Term: rf.currentTerm,
					}
					// can still be locked over this, this loop doesn't wait
					for i := 0; i < len(rf.peers); i++ {
						// Get all votes out before we unlock
						// Network latency should be slower than this
						if i != rf.me {
							reply := RequestVoteReply{}
							go rf.sendRequestVote(i,&args,&reply)
						}
					}
					// things can reply once all are sent out
					rf.mu.Unlock()
					// Wait for votes, or timeout
					Loop:
					for { // need to loop until we can break, as we may need to get multiple votes
						select {
							case <-time.After(time.Duration(rand.Intn(100) + 600) * time.Millisecond):
								// Timeout, break and try again
								break Loop
							case <-rf.heartBeatChan:
								// Got a heartbeat, become follower (in handler)
								// Term will have been adjusted in handler
								break Loop
							case result:= <-rf.voteChan:
								rf.mu.Lock()
								// Check if valid vote
								if result.Term == rf.currentTerm && result.Vote {
									// Increment our vote count
									rf.votes += 1
									// Check if we have a majority
									if rf.votes * 2 > len(rf.peers) {
										// Become leader
										rf.status = LEADER
										for i := 0; i < len(rf.peers); i++ {
											// NextIndex catches follower's logs up
											// therefore we start optimistic and move
											// back if necessary
											rf.nextIndex[i] = rf.getLastLogIndex()+1
											// EntryReceived is to determine our highest
											// commitlevel is, so we start pessimistic
											// until we can confirm a log has received this
											// entry index, in which we could commit it
											rf.entryReceived[i] = -1
											// Basically, entryreceived stays at -1 until
											// follower's log is caught up.  Then it converges
											// with nextindex
										}
										rf.mu.Unlock()
										break Loop
									}
								}
								// on else cases, ignore votes
								// this will flush out the channel of old votes as well
								rf.mu.Unlock()
						}
						time.Sleep(10 * time.Millisecond)
					}
				case LEADER:
					// Check to see if we can increment our commitlevel
					count := len(rf.peers) // set so it enters
					for count*2 > len(rf.peers) {
						count = 0
						for i := 0; i < len(rf.peers); i++ {
							if rf.entryReceived[i] > rf.committedEntry {
								count++
							}
						}
						if count*2 > len(rf.peers) {
							rf.committedEntry++
							// has to step up every time in order to find
							// the minimum that we have a majority on
						}
					}
					
					// Do append entries
					for i := 0; i < len(rf.peers); i++ {
						if i != rf.me { // Don't send to self
							var lastLogTerm int
							// keep track of each follower's lastlogindex
							// with nextIndex
							if rf.nextIndex[i]-1 >= 0 {
								// Have something to put
								lastLogTerm = rf.log[rf.nextIndex[i]-1].Term
							} else {
								lastLogTerm = -1
							}
							args := AppendEntryArgs{
								// nextindex is 0 if no one has entries
								LastLogIndex: rf.nextIndex[i]-1,
								LastLogTerm: lastLogTerm,
								Entries: make([]Command, 0), // start an empty array
								CommitLevel: rf.committedEntry,
								Term: rf.currentTerm,
								Leader: rf.me,
								// Term
							}
							for j := rf.nextIndex[i]; j <= rf.getLastLogIndex(); j++ {
								// Add all entries from where we're checking until now
								args.Entries = append(args.Entries, rf.log[j])
							}
							reply := AppendEntryReply{}
							go rf.sendAppendEntry(i,&args,&reply)
						}
					}
					rf.mu.Unlock()

					time.Sleep(time.Duration(100) * time.Millisecond)
			}
		}
	}()
	return rf
}

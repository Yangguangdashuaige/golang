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
	"6.824/labgob"
	"bytes"
	"fmt"
	"math/rand"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"

	"6.824/labrpc"
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// Status 节点的角色
type Status int

// VoteState 投票的状态 2A
type VoteState int

// AppendEntriesState 追加日志的状态 2A 2B
type AppendEntriesState int

// HeartBeatTimeout 定义一个全局心跳超时时间
var HeartBeatTimeout = 120 * time.Millisecond

const (
	Follower Status = iota
	Candidate
	Leader
)

const (
	Normal VoteState = iota
	Killed
	Expire
	Voted
)

const (
	AppNormal AppendEntriesState = iota
	AppOutOfDate
	AppKilled
	AppRepeat
	Mismatch
	AppCommitted
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu          sync.Mutex
	peers       []*labrpc.ClientEnd
	persister   *Persister
	me          int
	dead        int32
	currentTerm int
	votedFor    int
	logs        []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	status   Status
	overtime time.Duration
	timer    *time.Ticker

	applyChan chan ApplyMsg
}

type LogEntry struct {
	Term    int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) G1etState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.status == Leader
}
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	//fmt.Println("the peer[", rf.me, "] state is:", rf.state)
	if rf.status == Leader {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil {
		fmt.Println("decode error")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
	}
}

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	return true
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// RequestVoteArgs
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
	VoteState   VoteState
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 当前节点crash
	if rf.killed() {
		reply.VoteState = Killed
		reply.Term = -1
		reply.VoteGranted = false
		return
	}
	if args.Term < rf.currentTerm {
		reply.VoteState = Expire
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		// 重置自身的状态
		rf.status = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	if rf.votedFor == -1 {
		currentLogIndex := len(rf.logs) - 1
		currentLogTerm := 0
		if currentLogIndex >= 0 {
			currentLogTerm = rf.logs[currentLogIndex].Term
		}

		if args.LastLogTerm < currentLogTerm || (len(rf.logs) > 0 && args.LastLogTerm == rf.logs[len(rf.logs)-1].Term && args.LastLogIndex < len(rf.logs)) {

			reply.VoteState = Expire
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
			return
		}

		rf.votedFor = args.CandidateId

		reply.VoteState = Normal
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.timer.Reset(rf.overtime)
	} else {
		reply.VoteState = Voted
		reply.VoteGranted = false
		if rf.votedFor != args.CandidateId {
			return
		} else {
			rf.status = Follower
		}
		rf.timer.Reset(rf.overtime)
	}
	return
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, voteNums *int) bool {
	if rf.killed() {
		return false
	}
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	for !ok {
		if rf.killed() {
			return false
		}
		ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		return false
	}
	switch reply.VoteState {
	case Expire:
		{
			rf.status = Follower
			rf.timer.Reset(rf.overtime)
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.votedFor = -1
			}
		}
	case Normal, Voted:
		if reply.VoteGranted && reply.Term == rf.currentTerm && *voteNums <= (len(rf.peers)/2) {
			*voteNums++
		}
		if *voteNums >= (len(rf.peers)/2)+1 {
			*voteNums = 0
			if rf.status == Leader {
				return ok
			}
			rf.status = Leader
			rf.nextIndex = make([]int, len(rf.peers))
			for i, _ := range rf.nextIndex {
				rf.nextIndex[i] = len(rf.logs) + 1
			}
			rf.timer.Reset(HeartBeatTimeout)
		}
	case Killed:
		return false
	}
	return ok
}

// AppendEntriesArgs 由leader复制log条目，也可以当做是心跳连接，注释中的rf为leader节点
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term        int
	Success     bool
	AppState    AppendEntriesState
	UpNextIndex int
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, appendNums *int) {

	if rf.killed() {
		return
	}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	for !ok {
		if rf.killed() {
			return
		}
		ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)

	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	switch reply.AppState {

	case AppKilled:
		{
			return
		}

	case AppNormal:
		{
			if reply.Success && reply.Term == rf.currentTerm && *appendNums <= len(rf.peers)/2 {
				*appendNums++
			}
			if rf.nextIndex[server] > len(rf.logs)+1 {
				return
			}
			rf.nextIndex[server] += len(args.Entries)
			if *appendNums > len(rf.peers)/2 {
				*appendNums = 0
				if len(rf.logs) == 0 || rf.logs[len(rf.logs)-1].Term != rf.currentTerm {
					return
				}
				for rf.lastApplied < len(rf.logs) {
					rf.lastApplied++
					applyMsg := ApplyMsg{
						CommandValid: true,
						Command:      rf.logs[rf.lastApplied-1].Command,
						CommandIndex: rf.lastApplied,
					}
					rf.applyChan <- applyMsg
					rf.commitIndex = rf.lastApplied
				}
				rf.persist()

			}
			return
		}

	case Mismatch, AppCommitted:
		if reply.Term > rf.currentTerm {
			rf.status = Follower
			rf.votedFor = -1
			rf.timer.Reset(rf.overtime)
			rf.currentTerm = reply.Term
			rf.persist()
		}
		rf.nextIndex[server] = reply.UpNextIndex
	case AppOutOfDate:
		rf.status = Follower
		rf.votedFor = -1
		rf.timer.Reset(rf.overtime)
		rf.currentTerm = reply.Term
		rf.persist()
	}
	return
}

// AppendEntries 建立心跳、同步日志RPC
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		reply.AppState = AppKilled
		reply.Term = -1
		reply.Success = false
		return
	}
	if args.Term < rf.currentTerm {
		reply.AppState = AppOutOfDate
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.persist()
		return
	}

	if args.PrevLogIndex != -1 && rf.lastApplied > args.PrevLogIndex {
		reply.AppState = AppCommitted
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.UpNextIndex = rf.lastApplied + 1
		rf.persist()
		return
	}

	if args.PrevLogIndex > 0 && (rf.lastApplied < args.PrevLogIndex) {
		reply.AppState = Mismatch
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.UpNextIndex = rf.lastApplied + 1
		rf.persist()
		return
	}
	if args.PrevLogIndex > 0 && (rf.logs[args.PrevLogIndex-1].Term != args.PrevLogTerm) {
		reply.AppState = Mismatch
		reply.Term = rf.currentTerm
		reply.Success = false
		ConflictTerm := rf.logs[args.PrevLogIndex-1].Term
		for index := 1; index <= args.PrevLogIndex; index++ {
			if rf.logs[index-1].Term == ConflictTerm {
				reply.UpNextIndex = index
				break
			}
		}

		rf.persist()
		return
	}

	rf.currentTerm = args.Term
	rf.votedFor = args.LeaderId
	rf.status = Follower
	rf.timer.Reset(rf.overtime)

	reply.AppState = AppNormal
	reply.Term = rf.currentTerm
	reply.Success = true

	if args.Entries != nil {
		rf.logs = rf.logs[:args.PrevLogIndex]
		rf.logs = append(rf.logs, args.Entries...)

	}
	rf.persist()

	for rf.lastApplied < args.LeaderCommit {
		rf.lastApplied++
		applyMsg := ApplyMsg{
			CommandValid: true,
			CommandIndex: rf.lastApplied,
			Command:      rf.logs[rf.lastApplied-1].Command,
		}
		rf.applyChan <- applyMsg
		rf.commitIndex = rf.lastApplied
	}

	return
}

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
	// 如果不是leader，直接返回
	if rf.status != Leader {
		return index, term, false
	}

	isLeader = true
	// 初始化日志条目。并进行追加
	appendLog := LogEntry{Term: rf.currentTerm, Command: command}
	rf.logs = append(rf.logs, appendLog)
	index = len(rf.logs)
	term = rf.currentTerm

	return index, term, isLeader

}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.mu.Lock()
	rf.timer.Stop()
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		select {

		case <-rf.timer.C:
			if rf.killed() {
				return
			}
			rf.mu.Lock()
			switch rf.status {

			// follower变成竞选者
			case Follower:
				rf.status = Candidate
				fallthrough
			case Candidate:

				rf.currentTerm += 1
				rf.votedFor = rf.me
				votedNums := 1

				rf.overtime = time.Duration(150+rand.Intn(200)) * time.Millisecond // 随机产生200-400ms
				rf.timer.Reset(rf.overtime)
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}

					voteArgs := RequestVoteArgs{
						Term:         rf.currentTerm,
						CandidateId:  rf.me,
						LastLogIndex: len(rf.logs),
						LastLogTerm:  0,
					}
					if len(rf.logs) > 0 {
						voteArgs.LastLogTerm = rf.logs[len(rf.logs)-1].Term
					}

					voteReply := RequestVoteReply{}

					go rf.sendRequestVote(i, &voteArgs, &voteReply, &votedNums)
				}
			case Leader:

				// 进行心跳
				appendNums := 1
				rf.timer.Reset(HeartBeatTimeout)
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					args := AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: 0,
						PrevLogTerm:  0,
						Entries:      nil,
						LeaderCommit: rf.commitIndex,
					}

					reply := AppendEntriesReply{}

					args.Entries = rf.logs[rf.nextIndex[i]-1:]

					if rf.nextIndex[i] > 0 {
						args.PrevLogIndex = rf.nextIndex[i] - 1
					}
					if args.PrevLogIndex > 0 {
						args.PrevLogTerm = rf.logs[args.PrevLogIndex-1].Term
					}
					go rf.sendAppendEntries(i, &args, &reply, &appendNums)
				}
			}

			rf.mu.Unlock()
		}

	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// Your initialization code here (2A, 2B, 2C).
	// 对应论文中的初始化
	rf.applyChan = applyCh //2B
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]LogEntry, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.status = Follower
	rf.overtime = time.Duration(150+rand.Intn(200)) * time.Millisecond
	rf.timer = time.NewTicker(rf.overtime)
	// initialize from state persisted before a crash

	rf.mu.Lock()
	rf.readPersist(persister.ReadRaftState())
	rf.mu.Unlock()
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

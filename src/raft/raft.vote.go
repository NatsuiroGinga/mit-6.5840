package raft

import (
	"sync"

	"6.5840/kvraft"
)

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

func (rf *Raft) candidateRequestVote(serverId int, args *RequestVoteArgs, voteCounter *int, becomeLeader *sync.Once) {
	kvraft.DPrintf("[%d]: term %v send vote request to %d\n", rf.me, args.Term, serverId)
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(serverId, args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > args.Term {
		kvraft.DPrintf("[%d]: %d 在新的term，更新term，结束\n", rf.me, serverId)
		rf.setNewTerm(reply.Term)
		return
	}
	if reply.Term < args.Term {
		kvraft.DPrintf("[%d]: %d 的term %d 已经失效，结束\n", rf.me, serverId, reply.Term)
		return
	}
	if !reply.VoteGranted {
		kvraft.DPrintf("[%d]: %d 没有投给me，结束\n", rf.me, serverId)
		return
	}
	kvraft.DPrintf("[%d]: from %d term一致，且投给%d\n", rf.me, serverId, rf.me)

	*voteCounter++

	if *voteCounter > len(rf.peers)/2 &&
		rf.currentTerm == args.Term &&
		rf.state == Candidate {
		kvraft.DPrintf("[%d]: 获得多数选票，可以提前结束\n", rf.me)
		becomeLeader.Do(func() {
			kvraft.DPrintf("[%d]: 当前term %d 结束\n", rf.me, rf.currentTerm)
			rf.state = Leader
			lastLogIndex := rf.log.lastLog().Index
			for i, _ := range rf.peers {
				rf.nextIndex[i] = lastLogIndex + 1
				rf.matchIndex[i] = 0
			}
			kvraft.DPrintf("[%d]: leader - nextIndex %#v", rf.me, rf.nextIndex)
			rf.appendEntries(true)
		})
	}
}

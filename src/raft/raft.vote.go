package raft

import (
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

func (rf *Raft) candidateRequestVote(serverId int, args *RequestVoteArgs, voteCh chan<- struct{}) {
	kvraft.DPrintf("[%d]: (term %d) 向 %d 请求投票", rf.me, rf.currentTerm, serverId)
	// 1. send RequestVote RPCs to all other servers
	reply := new(RequestVoteReply)
	ok := rf.sendRequestVote(serverId, args, reply)
	if !ok {
		kvraft.DPrintf("[%d]: (term %d) 向 %d 请求投票失败", rf.me, rf.currentTerm, serverId)
		return
	}
	// 2. handle reply
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		kvraft.DPrintf("[%d]: (term %d) 收到 %d 的投票请求，但是自己的 term 过时，变为 follower", rf.me, rf.currentTerm, serverId)
		rf.setNewTerm(reply.Term)
		return
	}

	if reply.Term < rf.currentTerm {
		kvraft.DPrintf("[%d]: %d 的term %d 已经失效，结束\n", rf.me, serverId, reply.Term)
		return
	}

	if !reply.VoteGranted {
		kvraft.DPrintf("[%d]: (term %d) %d 拒绝了投票请求", rf.me, rf.currentTerm, serverId)
		return
	}

	kvraft.DPrintf("[%d]: (term %d) %d 同意了投票请求", rf.me, rf.currentTerm, serverId)

	voteCh <- struct{}{}
}

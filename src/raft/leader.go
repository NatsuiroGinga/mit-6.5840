package raft

import (
	"math/rand"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isleader := rf.state == Leader
	return term, isleader
}

func (rf *Raft) resetElectionTimer() {
	t := time.Now()
	electionTimeout := time.Duration(150+rand.Intn(150)) * time.Millisecond
	rf.electionTime = t.Add(electionTimeout)
}

func (rf *Raft) setNewTerm(term int) {
	if term > rf.currentTerm || rf.currentTerm == 0 {
		rf.state = Follower
		rf.currentTerm = term
		rf.votedFor = -1
		// kvraft.DPrintf("[%d]: set term %v\n", rf.me, rf.currentTerm)
		log.Debug().Msgf("[%d]: set term %v\n", rf.me, rf.currentTerm)
		rf.persist()
	}
}

func (rf *Raft) leaderElection() {
	// 1. increment currentTerm
	rf.currentTerm++
	// 2. vote for self
	rf.votedFor = rf.me
	// 3. reset election timer
	rf.resetElectionTimer()
	// 4. persist
	rf.persist()
	// 5. state = candidate
	rf.state = Candidate
	// kvraft.DPrintf("[%d]: (term %d) 开始选举", rf.me, rf.currentTerm)
	log.Debug().Msgf("[%d]: (term %d) 开始选举", rf.me, rf.currentTerm)
	// 6. send RequestVote RPCs to all other servers
	args := &RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}
	voteCounter := 1
	voteCh := make(chan struct{})
	becomeLeader := new(sync.Once)
	for serverId := range rf.peers {
		if serverId != rf.me {
			go rf.candidateRequestVote(serverId, args, voteCh)
		}
	}
	// 7. count votes
	for range voteCh {
		voteCounter++
		rf.mu.Lock()
		if voteCounter > len(rf.peers)/2 && rf.currentTerm == args.Term && rf.state == Candidate {
			// kvraft.DPrintf("[%d]: (term %d) 获得多数选票，成为 leader", rf.me, rf.currentTerm)
			log.Debug().Msgf("[%d]: (term %d) 获得多数选票，成为 leader", rf.me, rf.currentTerm)
			becomeLeader.Do(func() {
				rf.leaderInit()
				rf.appendEntries(true)
			})
		}
		rf.mu.Unlock()
	}
}

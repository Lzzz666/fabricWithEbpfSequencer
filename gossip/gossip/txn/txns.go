/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package tnxstore

import (
	"sync"
	"time"

	"github.com/hyperledger/fabric/gossip/common"
)

var noopLock = func() {}

// Noop is a function that doesn't do anything
func Noop(_ interface{}) {
}

// invalidationTrigger is invoked on each message that was invalidated because of a message addition
// i.e: if add(0), add(1) was called one after the other, and the store has only {1} after the sequence of invocations
// then the invalidation trigger on 0 was called when 1 was added.
type invalidationTrigger func(message interface{})

// NewMessageStore returns a new MessageStore with the message replacing
// policy and invalidation trigger passed.
func NewTransactionStore(pol common.MessageReplacingPolicy, trigger invalidationTrigger) TransactionStore {
	return newTransactionStore(pol, trigger)
}

// NewMessageStoreExpirable returns a new MessageStore with the message replacing
// policy and invalidation trigger passed. It supports old message expiration after msgTTL, during expiration first external
// lock taken, expiration callback invoked and external lock released. Callback and external lock can be nil.
func NewTransactionStoreExpirable(pol common.MessageReplacingPolicy, trigger invalidationTrigger, msgTTL time.Duration, externalLock func(), externalUnlock func(), externalExpire func(interface{})) TransactionStore {
	store := newTransactionStore(pol, trigger)
	store.msgTTL = msgTTL

	if externalLock != nil {
		store.externalLock = externalLock
	}

	if externalUnlock != nil {
		store.externalUnlock = externalUnlock
	}

	if externalExpire != nil {
		store.expireMsgCallback = externalExpire
	}

	go store.expirationRoutine()
	return store
}

func newTransactionStore(pol common.MessageReplacingPolicy, trigger invalidationTrigger) *transactionStoreImpl {
	return &transactionStoreImpl{
		pol:        pol,
		txns:       make([]*txn, 0),
		invTrigger: trigger,

		externalLock:      noopLock,
		externalUnlock:    noopLock,
		expireMsgCallback: func(m interface{}) {},
		expiredCount:      0,

		doneCh: make(chan struct{}),
	}
}

// TransactionStore adds txns to an internal buffer.
// When a txn is received, it might:
//   - Be added to the buffer
//   - Discarded because of some txn already in the buffer (invalidated)
//   - Make a txn already in the buffer to be discarded (invalidates)
//
// When a txn is invalidated, the invalidationTrigger is invoked on that txn.
type TransactionStore interface {
	// add adds a txn to the store
	// returns true or false whether the txn was added to the store
	Add(txn interface{}) bool

	// Checks if txn is valid for insertion to store
	// returns true or false whether the txn can be added to the store
	CheckValid(txn interface{}) bool

	// size returns the amount of txns in the store
	Size() int

	// get returns all txns in the store
	Get() []interface{}

	// Stop all associated go routines
	Stop()

	// Purge purges all txns that are accepted by
	// the given predicate
	Purge(func(interface{}) bool)
}

type transactionStoreImpl struct {
	pol               common.MessageReplacingPolicy
	lock              sync.RWMutex
	txns              []*txn
	invTrigger        invalidationTrigger
	msgTTL            time.Duration
	expiredCount      int
	externalLock      func()
	externalUnlock    func()
	expireMsgCallback func(msg interface{})
	doneCh            chan struct{}
	stopOnce          sync.Once
}

type txn struct {
	data    interface{}
	created time.Time
	expired bool
}

// add adds a txn to the store
func (s *transactionStoreImpl) Add(transaction interface{}) bool {
	s.lock.Lock()
	defer s.lock.Unlock()

	n := len(s.txns)
	for i := 0; i < n; i++ {
		m := s.txns[i]
		switch s.pol(transaction, m.data) {
		case common.MessageInvalidated:
			return false
		case common.MessageInvalidates:
			s.invTrigger(m.data)
			s.txns = append(s.txns[:i], s.txns[i+1:]...)
			n--
			i--
		}
	}

	s.txns = append(s.txns, &txn{data: transaction, created: time.Now()})
	return true
}

func (s *transactionStoreImpl) Purge(shouldBePurged func(interface{}) bool) {
	shouldMsgBePurged := func(m *txn) bool {
		return shouldBePurged(m.data)
	}
	if !s.isPurgeNeeded(shouldMsgBePurged) {
		return
	}
	s.lock.Lock()
	defer s.lock.Unlock()
	n := len(s.txns)
	for i := 0; i < n; i++ {
		if !shouldMsgBePurged(s.txns[i]) {
			continue
		}
		s.invTrigger(s.txns[i].data)
		s.txns = append(s.txns[:i], s.txns[i+1:]...)
		n--
		i--
	}
}

// Checks if txn is valid for insertion to store
func (s *transactionStoreImpl) CheckValid(transaction interface{}) bool {
	s.lock.RLock()
	defer s.lock.RUnlock()

	for _, m := range s.txns {
		if s.pol(transaction, m.data) == common.MessageInvalidated {
			return false
		}
	}
	return true
}

// size returns the amount of txns in the store
func (s *transactionStoreImpl) Size() int {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return len(s.txns) - s.expiredCount
}

// get returns all txns in the store
func (s *transactionStoreImpl) Get() []interface{} {
	res := make([]interface{}, 0)

	s.lock.RLock()
	defer s.lock.RUnlock()

	for _, msg := range s.txns {
		if !msg.expired {
			res = append(res, msg.data)
		}
	}
	return res
}

func (s *transactionStoreImpl) expireMessages() {
	s.externalLock()
	s.lock.Lock()
	defer s.lock.Unlock()
	defer s.externalUnlock()

	n := len(s.txns)
	for i := 0; i < n; i++ {
		m := s.txns[i]
		if !m.expired {
			if time.Since(m.created) > s.msgTTL {
				m.expired = true
				s.expireMsgCallback(m.data)
				s.expiredCount++
			}
		} else {
			if time.Since(m.created) > (s.msgTTL * 2) {
				s.txns = append(s.txns[:i], s.txns[i+1:]...)
				n--
				i--
				s.expiredCount--
			}
		}
	}
}

func (s *transactionStoreImpl) isPurgeNeeded(shouldBePurged func(*txn) bool) bool {
	s.lock.RLock()
	defer s.lock.RUnlock()
	for _, m := range s.txns {
		if shouldBePurged(m) {
			return true
		}
	}
	return false
}

func (s *transactionStoreImpl) expirationRoutine() {
	for {
		select {
		case <-s.doneCh:
			return
		case <-time.After(s.expirationCheckInterval()):
			hasTxnExpired := func(m *txn) bool {
				if !m.expired && time.Since(m.created) > s.msgTTL {
					return true
				} else if time.Since(m.created) > (s.msgTTL * 2) {
					return true
				}
				return false
			}
			if s.isPurgeNeeded(hasTxnExpired) {
				s.expireMessages()
			}
		}
	}
}

func (s *transactionStoreImpl) Stop() {
	stopFunc := func() {
		close(s.doneCh)
	}
	s.stopOnce.Do(stopFunc)
}

func (s *transactionStoreImpl) expirationCheckInterval() time.Duration {
	return s.msgTTL / 100
}

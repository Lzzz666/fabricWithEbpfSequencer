/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package txnstore

import (
	"fmt"
	"sync"
	"time"

	"github.com/hyperledger/fabric/gossip/common"
	"github.com/hyperledger/fabric/protoutil"
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
		txns:       make(map[string]*txn),
		txnOrder:   make([]string, 0),
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
	Add(txid string, txn interface{}) bool

	// Checks if txn is valid for insertion to store
	// returns true or false whether the txn can be added to the store
	CheckValid(txid string, transaction interface{}) bool

	// size returns the amount of txns in the store
	Size() int

	// get returns all txns in the store
	Get() []interface{}

	// Stop all associated go routines
	Stop()

	// Purge purges all txns that are accepted by
	// the given predicate
	Purge(func(interface{}) bool)

	// GetByID 根据交易 ID 获取特定交易 (新增方法)
	GetByID(id string) (interface{}, bool)

	// Contains 检查是否包含特定 ID 的交易 (新增方法)
	Contains(id string) bool

	// Remove 根据交易 ID 移除特定交易 (新增方法)
	Remove(id string) bool
}

type transactionStoreImpl struct {
	pol               common.MessageReplacingPolicy
	lock              sync.RWMutex
	txns              map[string]*txn
	txnOrder          []string
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
	id      string
	data    interface{}
	created time.Time
	expired bool
}

// extractTxIDFromPayload 从 Payload 字节数组中提取 txid
func extractTxIDFromPayload(payloadBytes []byte) (string, error) {
	// 反序列化 Payload
	payload, err := protoutil.UnmarshalPayload(payloadBytes)
	if err != nil {
		return "", err
	}

	if payload.Header == nil {
		return "", fmt.Errorf("payload header is nil")
	}

	// 反序列化 ChannelHeader
	chdr, err := protoutil.UnmarshalChannelHeader(payload.Header.ChannelHeader)
	if err != nil {
		return "", err
	}

	// 如果 ChannelHeader 中有 TxId，直接返回
	if chdr.TxId != "" {
		return chdr.TxId, nil
	}

	// 如果没有 TxId，尝试从 SignatureHeader 中计算
	sighdr, err := protoutil.UnmarshalSignatureHeader(payload.Header.SignatureHeader)
	if err != nil {
		return "", err
	}

	// 使用 nonce 和 creator 计算 txid
	txid := protoutil.ComputeTxID(sighdr.Nonce, sighdr.Creator)
	return txid, nil
}

// getTxnID 从交易数据中提取唯一标识符
func getTxnID(transaction interface{}) string {
	// 检查是否是字节数组（Payload）
	if payloadBytes, ok := transaction.([]byte); ok {
		// 尝试从 Payload 中提取 txid
		if txid, err := extractTxIDFromPayload(payloadBytes); err == nil {
			return txid
		}
	}

	// 如果无法提取 txid，使用内存地址作为后备方案
	return fmt.Sprintf("%p", transaction)
}

// add adds a whole txn to the store
func (s *transactionStoreImpl) Add(txid string, transaction interface{}) bool {
	s.lock.Lock()
	defer s.lock.Unlock()

	// 直接使用提供的 txid 作為 key，而不是從 transaction 中提取
	id := txid

	// 檢查交易是否已存在
	if _, exists := s.txns[id]; exists {
		return false // 交易已存在，不添加
	}

	// 檢查新交易是否會使現有交易無效
	var toRemove []string
	for existingID, existingTxn := range s.txns {
		switch s.pol(transaction, existingTxn.data) {
		case common.MessageInvalidated:
			return false // 新交易被現有交易無效化
		case common.MessageInvalidates:
			toRemove = append(toRemove, existingID) // 新交易使現有交易無效化
		}
	}

	// 移除被无效化的交易
	for _, removeID := range toRemove {
		delete(s.txns, removeID)
		// 从顺序列表中移除
		for i, orderID := range s.txnOrder {
			if orderID == removeID {
				s.txnOrder = append(s.txnOrder[:i], s.txnOrder[i+1:]...)
				break
			}
		}
	}

	s.txns[id] = &txn{id: id, data: transaction, created: time.Now()}
	s.txnOrder = append(s.txnOrder, id)
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

	var newOrder []string
	for _, id := range s.txnOrder {
		m := s.txns[id]
		if shouldMsgBePurged(m) {
			s.invTrigger(m.data)
			delete(s.txns, id)
		} else {
			newOrder = append(newOrder, id)
		}
	}
	s.txnOrder = newOrder
}

// Checks if txn is valid for insertion to store
func (s *transactionStoreImpl) CheckValid(txid string, transaction interface{}) bool {
	s.lock.RLock()
	defer s.lock.RUnlock()

	// 直接使用提供的 txid 作為 key
	id := txid
	if _, exists := s.txns[id]; exists {
		return false // 交易已存在
	}

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

	for _, id := range s.txnOrder {
		m := s.txns[id]
		if !m.expired {
			res = append(res, m.data)
		}
	}
	return res
}

func (s *transactionStoreImpl) expireMessages() {
	s.externalLock()
	s.lock.Lock()
	defer s.lock.Unlock()
	defer s.externalUnlock()

	var newOrder []string
	for _, id := range s.txnOrder {
		m := s.txns[id]
		if !m.expired {
			if time.Since(m.created) > s.msgTTL {
				m.expired = true
				s.expireMsgCallback(m.data)
				s.expiredCount++
				newOrder = append(newOrder, id)
			} else {
				newOrder = append(newOrder, id)
			}
		} else {
			if time.Since(m.created) > (s.msgTTL * 2) {
				delete(s.txns, id)
				s.expiredCount--
			} else {
				newOrder = append(newOrder, id)
			}
		}
	}
	s.txnOrder = newOrder
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

// GetByID 根据交易 ID 获取特定交易 (新增方法)
func (s *transactionStoreImpl) GetByID(id string) (interface{}, bool) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	txn, exists := s.txns[id]
	if !exists || txn == nil {
		return nil, false
	}
	return txn.data, true
}

// Contains 检查是否包含特定 ID 的交易 (新增方法)
func (s *transactionStoreImpl) Contains(id string) bool {
	s.lock.RLock()
	defer s.lock.RUnlock()
	_, exists := s.txns[id]
	return exists
}

// Remove 根据交易 ID 移除特定交易 (新增方法)
func (s *transactionStoreImpl) Remove(id string) bool {
	s.lock.Lock()
	defer s.lock.Unlock()

	// 檢查交易是否存在
	txn, exists := s.txns[id]
	if !exists {
		return false // 交易不存在，無法移除
	}

	// 從 map 中刪除交易
	delete(s.txns, id)

	// 從順序列表中移除對應的 ID
	for i, orderID := range s.txnOrder {
		if orderID == id {
			s.txnOrder = append(s.txnOrder[:i], s.txnOrder[i+1:]...)
			break
		}
	}

	// 如果交易已過期，需要調整過期計數
	if txn.expired {
		s.expiredCount--
	}

	return true // 成功移除
}

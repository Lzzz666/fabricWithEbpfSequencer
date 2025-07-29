/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package privdata

import (
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/ledger/rwset"
	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	protostransientstore "github.com/hyperledger/fabric-protos-go-apiv2/transientstore"
	"github.com/hyperledger/fabric/common/channelconfig"
	"github.com/hyperledger/fabric/core/committer"
	"github.com/hyperledger/fabric/core/committer/txvalidator"
	"github.com/hyperledger/fabric/core/common/privdata"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/rwsetutil"
	"github.com/hyperledger/fabric/core/transientstore"
	txnstore "github.com/hyperledger/fabric/gossip/gossip/txn"
	"github.com/hyperledger/fabric/gossip/metrics"
	privdatacommon "github.com/hyperledger/fabric/gossip/privdata/common"
	"github.com/hyperledger/fabric/gossip/util"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
)

const pullRetrySleepInterval = time.Second

var logger = util.GetLogger(util.PrivateDataLogger, "")

//go:generate mockery --dir . --name CollectionStore --case underscore --output mocks/

// CollectionStore is the local interface used to generate mocks for foreign interface.
type CollectionStore interface {
	privdata.CollectionStore
}

//go:generate mockery --dir . --name Committer --case underscore --output mocks/

// Committer is the local interface used to generate mocks for foreign interface.
type Committer interface {
	committer.Committer
}

// Coordinator orchestrates the flow of the new
// blocks arrival and in flight transient data, responsible
// to complete missing parts of transient data for given block.
type Coordinator interface {
	// StoreBlock deliver new block with underlined private data
	// returns missing transaction ids
	StoreBlock(block *common.Block, data util.PvtDataCollections) error

	// StorePvtData used to persist private data into transient store
	StorePvtData(txid string, privData *protostransientstore.TxPvtReadWriteSetWithConfigInfo, blckHeight uint64) error

	// GetPvtDataAndBlockByNum gets block by number and also returns all related private data
	// that requesting peer is eligible for.
	// The order of private data in slice of PvtDataCollections doesn't imply the order of
	// transactions in the block related to these private data, to get the correct placement
	// need to read TxPvtData.SeqInBlock field
	GetPvtDataAndBlockByNum(seqNum uint64, peerAuth protoutil.SignedData) (*common.Block, util.PvtDataCollections, error)

	// Get recent block sequence number
	LedgerHeight() (uint64, error)

	// Close coordinator, shuts down coordinator service
	Close()
}

type dig2sources map[privdatacommon.DigKey][]*peer.Endorsement

func (d2s dig2sources) keys() []privdatacommon.DigKey {
	res := make([]privdatacommon.DigKey, 0, len(d2s))
	for dig := range d2s {
		res = append(res, dig)
	}
	return res
}

// Fetcher interface which defines API to fetch missing
// private data elements
type Fetcher interface {
	fetch(dig2src dig2sources) (*privdatacommon.FetchedPvtDataContainer, error)
}

//go:generate mockery --dir ./ --name CapabilityProvider --case underscore --output mocks/

// CapabilityProvider contains functions to retrieve capability information for a channel
type CapabilityProvider interface {
	// Capabilities defines the capabilities for the application portion of this channel
	Capabilities() channelconfig.ApplicationCapabilities
}

// Support encapsulates set of interfaces to
// aggregate required functionality by single struct
type Support struct {
	ChainID string
	privdata.CollectionStore
	txvalidator.Validator
	committer.Committer
	Fetcher
	CapabilityProvider
}

// CoordinatorConfig encapsulates the config that is passed to a new coordinator
type CoordinatorConfig struct {
	// TransientBlockRetention indicates the number of blocks to retain in the transient store
	// when purging below height on committing every TransientBlockRetention-th block
	TransientBlockRetention uint64
	// PullRetryThreshold indicates the max duration an attempted fetch from a remote peer will retry
	// for before giving up and leaving the private data as missing
	PullRetryThreshold time.Duration
	// SkipPullingInvalidTransactions if true will skip the fetch from remote peer step for transactions
	// marked as invalid
	SkipPullingInvalidTransactions bool
}

type coordinator struct {
	mspID          string
	selfSignedData protoutil.SignedData
	Support
	store                          *transientstore.Store
	txnStore                       txnstore.TransactionStore
	transientBlockRetention        uint64
	logger                         util.Logger
	metrics                        *metrics.PrivdataMetrics
	pullRetryThreshold             time.Duration
	skipPullingInvalidTransactions bool
	idDeserializerFactory          IdentityDeserializerFactory
}

// NewCoordinator creates a new instance of coordinator
func NewCoordinator(mspID string, support Support, store *transientstore.Store, txnStore txnstore.TransactionStore, selfSignedData protoutil.SignedData, metrics *metrics.PrivdataMetrics,
	config CoordinatorConfig, idDeserializerFactory IdentityDeserializerFactory) Coordinator {
	return &coordinator{
		Support:                        support,
		mspID:                          mspID,
		store:                          store,
		txnStore:                       txnStore,
		selfSignedData:                 selfSignedData,
		transientBlockRetention:        config.TransientBlockRetention,
		logger:                         logger.With("channel", support.ChainID),
		metrics:                        metrics,
		pullRetryThreshold:             config.PullRetryThreshold,
		skipPullingInvalidTransactions: config.SkipPullingInvalidTransactions,
		idDeserializerFactory:          idDeserializerFactory,
	}
}

// StoreBlock stores block with private data into the ledger
func (c *coordinator) StoreBlock(block *common.Block, privateDataSets util.PvtDataCollections) error {
	if block.Data == nil {
		return errors.New("Block data is empty")
	}
	if block.Header == nil {
		return errors.New("Block header is nil")
	}

	c.logger.Infof("Received block [%d] from buffer", block.Header.Number)

	c.logger.Debugf("Validating block [%d]", block.Header.Number)

	c.logger.Warningf("[Debug by lz] block: %v", block)
	// TODO: 組合 不完整的 block 和 完整的 block 的資料
	// 先 get peer 有的 private data

	validationStart := time.Now()
	err := c.Validator.Validate(block)
	c.reportValidationDuration(time.Since(validationStart))
	if err != nil {
		c.logger.Errorf("Validation failed: %+v", err)
		return err
	}
	// 在這裡要取得 txnstore 中的完整交易
	blockAndPvtData := &ledger.BlockAndPvtData{
		Block:          block,
		PvtData:        make(ledger.TxPvtDataMap),
		MissingPvtData: make(ledger.TxMissingPvtData),
	}

	exist, err := c.DoesPvtDataInfoExistInLedger(block.Header.Number)
	if err != nil {
		return err
	}
	if exist {
		commitOpts := &ledger.CommitOptions{FetchPvtDataFromLedger: true}
		return c.CommitLegacy(blockAndPvtData, commitOpts)
	}

	listMissingPrivateDataDurationHistogram := c.metrics.ListMissingPrivateDataDuration.With("channel", c.ChainID)
	fetchDurationHistogram := c.metrics.FetchDuration.With("channel", c.ChainID)
	purgeDurationHistogram := c.metrics.PurgeDuration.With("channel", c.ChainID)
	pdp := &PvtdataProvider{
		mspID:                                   c.mspID,
		selfSignedData:                          c.selfSignedData,
		logger:                                  logger.With("channel", c.ChainID),
		listMissingPrivateDataDurationHistogram: listMissingPrivateDataDurationHistogram,
		fetchDurationHistogram:                  fetchDurationHistogram,
		purgeDurationHistogram:                  purgeDurationHistogram,
		transientStore:                          c.store,
		pullRetryThreshold:                      c.pullRetryThreshold,
		prefetchedPvtdata:                       privateDataSets,
		transientBlockRetention:                 c.transientBlockRetention,
		channelID:                               c.ChainID,
		blockNum:                                block.Header.Number,
		storePvtdataOfInvalidTx:                 c.Support.CapabilityProvider.Capabilities().StorePvtDataOfInvalidTx(),
		skipPullingInvalidTransactions:          c.skipPullingInvalidTransactions,
		fetcher:                                 c.Fetcher,
		idDeserializerFactory:                   c.idDeserializerFactory,
	}
	// 這裡是取得 block 中 要寫入 private data 的 transaction 的資訊
	// 如何這裡的 block 解開是只有 txid 的話，那麼就從 transaction store 中取得完整的交易並處理 （已完成）
	pvtdataToRetrieve, err := c.getTxPvtdataInfoFromBlock(block)
	if err != nil {
		c.logger.Warningf("Failed to get private data info from block: %s", err)
		return err
	}

	// Retrieve the private data.
	// RetrievePvtdata checks this peer's eligibility and then retreives from cache, transient store, or from a remote peer.
	retrievedPvtdata, err := pdp.RetrievePvtdata(pvtdataToRetrieve)
	if err != nil {
		c.logger.Warningf("Failed to retrieve pvtdata: %s", err)
		return err
	}

	blockAndPvtData.PvtData = retrievedPvtdata.blockPvtdata.PvtData
	blockAndPvtData.MissingPvtData = retrievedPvtdata.blockPvtdata.MissingPvtData
	logger.Warningf("[Debug by lz] blockAndPvtData.PvtData in StoreBlock: %v", blockAndPvtData.PvtData)
	logger.Warningf("[Debug by lz] blockAndPvtData.MissingPvtData in StoreBlock: %v", blockAndPvtData.MissingPvtData)

	logger.Warningf("[Debug by lz] blockAndPvtData in StoreBlock: %v", blockAndPvtData)
	// commit block and private data
	commitStart := time.Now()
	// here is the commit, and we have error here
	err = c.CommitLegacy(blockAndPvtData, &ledger.CommitOptions{})
	c.reportCommitDuration(time.Since(commitStart))
	if err != nil {
		return errors.Wrap(err, "commit failed")
	}

	// Purge transactions
	go retrievedPvtdata.Purge()

	return nil
}

// StorePvtData used to persist private data into transient store
func (c *coordinator) StorePvtData(txID string, privData *protostransientstore.TxPvtReadWriteSetWithConfigInfo, blkHeight uint64) error {
	return c.store.Persist(txID, blkHeight, privData)
}

// GetPvtDataAndBlockByNum gets block by number and also returns all related private data
// that requesting peer is eligible for.
// The order of private data in slice of PvtDataCollections doesn't imply the order of
// transactions in the block related to these private data, to get the correct placement
// need to read TxPvtData.SeqInBlock field
func (c *coordinator) GetPvtDataAndBlockByNum(seqNum uint64, peerAuthInfo protoutil.SignedData) (*common.Block, util.PvtDataCollections, error) {
	blockAndPvtData, err := c.Committer.GetPvtDataAndBlockByNum(seqNum)
	if err != nil {
		return nil, nil, err
	}

	seqs2Namespaces := aggregatedCollections{}
	for seqInBlock := range blockAndPvtData.Block.Data.Data {
		txPvtDataItem, exists := blockAndPvtData.PvtData[uint64(seqInBlock)]
		if !exists {
			continue
		}

		// Iterate through the private write sets and include them in response if requesting peer is eligible for it
		for _, ns := range txPvtDataItem.WriteSet.NsPvtRwset {
			for _, col := range ns.CollectionPvtRwset {
				cc := privdata.CollectionCriteria{
					Channel:    c.ChainID,
					Namespace:  ns.Namespace,
					Collection: col.CollectionName,
				}
				sp, err := c.CollectionStore.RetrieveCollectionAccessPolicy(cc)
				if err != nil {
					c.logger.Warningf("Failed obtaining policy for collection criteria [%#v]: %s", cc, err)
					continue
				}
				isAuthorized := sp.AccessFilter()
				if isAuthorized == nil {
					c.logger.Warningf("Failed obtaining filter for collection criteria [%#v]", cc)
					continue
				}
				if !isAuthorized(peerAuthInfo) {
					c.logger.Debugf("Skipping collection criteria [%#v] because peer isn't authorized", cc)
					continue
				}
				seqs2Namespaces.addCollection(uint64(seqInBlock), txPvtDataItem.WriteSet.DataModel, ns.Namespace, col)
			}
		}
	}

	return blockAndPvtData.Block, seqs2Namespaces.asPrivateData(), nil
}

// getTxPvtdataInfoFromBlock parses the block transactions and returns the list of private data items in the block.
// Note that this peer's eligibility for the private data is not checked here.
func (c *coordinator) getTxPvtdataInfoFromBlock(block *common.Block) ([]*ledger.TxPvtdataInfo, error) {
	c.logger.Warningf("[Debug by lz] getTxPvtdataInfoFromBlock")
	txPvtdataItemsFromBlock := []*ledger.TxPvtdataInfo{}

	if block.Metadata == nil || len(block.Metadata.Metadata) <= int(common.BlockMetadataIndex_TRANSACTIONS_FILTER) {
		return nil, errors.New("Block.Metadata is nil or Block.Metadata lacks a Tx filter bitmap")
	}
	txsFilter := txValidationFlags(block.Metadata.Metadata[common.BlockMetadataIndex_TRANSACTIONS_FILTER])
	data := block.Data.Data
	c.logger.Warningf("[Debug by lz]  getTxPvtdataInfoFromBlock: data: %v", data)
	if len(txsFilter) != len(block.Data.Data) {
		return nil, errors.Errorf("block data size(%d) is different from Tx filter size(%d)", len(block.Data.Data), len(txsFilter))
	}

	for seqInBlock, txEnvBytes := range data {
		invalid := txsFilter[seqInBlock] != uint8(peer.TxValidationCode_VALID)
		txInfo, err := c.getTxInfoFromTransactionBytes(txEnvBytes)
		if err != nil {
			continue
		}
		// 照理來說這裡應該已經要是從 txnstore 中取得的完整交易 （txInfo）
		logger.Warningf("[Debug by lz] txInfo in getTxPvtdataInfoFromBlock: %v", txInfo)
		logger.Warningf("[Debug by lz] txInfo.txRWSet in getTxPvtdataInfoFromBlock: %v", txInfo.txRWSet)
		logger.Warningf("[Debug by lz] txInfo.txRWSet.NsRwSets in getTxPvtdataInfoFromBlock: %v", txInfo.txRWSet.NsRwSets)
		logger.Warningf("[Debug by lz] txInfo.txid in getTxPvtdataInfoFromBlock: %v", txInfo.txID)
		colPvtdataInfo := []*ledger.CollectionPvtdataInfo{}
		for _, ns := range txInfo.txRWSet.NsRwSets {
			for _, hashedCollection := range ns.CollHashedRwSets {
				// skip if no writes
				if !containsWrites(txInfo.txID, ns.NameSpace, hashedCollection) {
					continue
				}
				cc := privdata.CollectionCriteria{
					Channel:    txInfo.channelID,
					Namespace:  ns.NameSpace,
					Collection: hashedCollection.CollectionName,
				}

				colConfig, err := c.CollectionStore.RetrieveCollectionConfig(cc)
				if err != nil {
					c.logger.Warningf("Failed to retrieve collection config for collection criteria [%#v]: %s", cc, err)
					return nil, err
				}
				col := &ledger.CollectionPvtdataInfo{
					Namespace:        ns.NameSpace,
					Collection:       hashedCollection.CollectionName,
					ExpectedHash:     hashedCollection.PvtRwSetHash,
					CollectionConfig: colConfig,
					Endorsers:        txInfo.endorsements,
				}
				colPvtdataInfo = append(colPvtdataInfo, col)
			}
		}
		txPvtdataToRetrieve := &ledger.TxPvtdataInfo{
			TxID:                  txInfo.txID,
			Invalid:               invalid,
			SeqInBlock:            uint64(seqInBlock),
			CollectionPvtdataInfo: colPvtdataInfo,
		}
		txPvtdataItemsFromBlock = append(txPvtdataItemsFromBlock, txPvtdataToRetrieve)
	}

	return txPvtdataItemsFromBlock, nil
}

func (c *coordinator) reportValidationDuration(time time.Duration) {
	c.metrics.ValidationDuration.With("channel", c.ChainID).Observe(time.Seconds())
}

func (c *coordinator) reportCommitDuration(time time.Duration) {
	c.metrics.CommitPrivateDataDuration.With("channel", c.ChainID).Observe(time.Seconds())
}

type seqAndDataModel struct {
	seq       uint64
	dataModel rwset.TxReadWriteSet_DataModel
}

// map from seqAndDataModel to:
//
//	map from namespace to []*rwset.CollectionPvtReadWriteSet
type aggregatedCollections map[seqAndDataModel]map[string][]*rwset.CollectionPvtReadWriteSet

func (ac aggregatedCollections) addCollection(seqInBlock uint64, dm rwset.TxReadWriteSet_DataModel, namespace string, col *rwset.CollectionPvtReadWriteSet) {
	seq := seqAndDataModel{
		dataModel: dm,
		seq:       seqInBlock,
	}
	if _, exists := ac[seq]; !exists {
		ac[seq] = make(map[string][]*rwset.CollectionPvtReadWriteSet)
	}
	ac[seq][namespace] = append(ac[seq][namespace], col)
}

func (ac aggregatedCollections) asPrivateData() []*ledger.TxPvtData {
	var data []*ledger.TxPvtData
	for seq, ns := range ac {
		txPrivateData := &ledger.TxPvtData{
			SeqInBlock: seq.seq,
			WriteSet: &rwset.TxPvtReadWriteSet{
				DataModel: seq.dataModel,
			},
		}
		for namespaceName, cols := range ns {
			txPrivateData.WriteSet.NsPvtRwset = append(txPrivateData.WriteSet.NsPvtRwset, &rwset.NsPvtReadWriteSet{
				Namespace:          namespaceName,
				CollectionPvtRwset: cols,
			})
		}
		data = append(data, txPrivateData)
	}
	return data
}

type txInfo struct {
	channelID    string
	txID         string
	endorsements []*peer.Endorsement
	txRWSet      *rwsetutil.TxRwSet
}

// getTxInfoFromTransactionBytes parses a transaction and returns info required for private data retrieval
// 這裡主要是處理 Ｂlock 進來後 要寫入 private data 的 transaction 的資訊
func (c *coordinator) getTxInfoFromTransactionBytes(envBytes []byte) (*txInfo, error) {
	txInfo := &txInfo{}
	logger.Warningf("[Debug by lz] getTxInfoFromTransactionBytes: envBytes: %v", envBytes)

	// 如果這個是完整的交易，那麼我們需要從 envBytes 中解析出 envelope
	env, err := protoutil.GetEnvelopeFromBlock(envBytes)
	// 如果這不是完整的交易，那就是從 orderer 來的 txid 而已
	if err != nil {
		logger.Warningf("Invalid envelope: %s", err)
		logger.Warningf("[Debug by lz] Invalid envelope, so it's a txid")
		// 如果這個是只有 txid，那麼我們需要從 transaction store 中取得完整的交易
		txID := string(envBytes)
		logger.Warningf("[Debug by lz] getTxInfoFromTransactionBytes txID: %s", txID)
		logger.Warningf("[Debug by lz] getTxInfoFromTransactionBytes txID hex: %x", envBytes)
		logger.Warningf("[Debug by lz] getTxInfoFromTransactionBytes txID length: %d", len(txID))

		// 從 transaction store 中根據 txID 獲取完整的交易
		if c.txnStore != nil {
			logger.Warningf("[Debug by lz] Mempool transactions: %v", c.txnStore.Get())
			logger.Warningf("[Debug by lz] Transaction store size: %d", c.txnStore.Size())

			// 嘗試多種可能的 txID 格式
			var fullTxnData interface{}
			var found bool

			// 1. 嘗試原始字符串格式
			fullTxnData, found = c.txnStore.GetByID(txID)
			if found {
				logger.Warningf("[Debug by lz] Found transaction using original txID: %s", txID)
			} else {
				logger.Warningf("[Debug by lz] Not found using original txID: %s", txID)

				// 2. 嘗試去除可能的空字節或特殊字符
				cleanTxID := strings.TrimSpace(strings.Trim(txID, "\x00"))
				if cleanTxID != txID {
					logger.Warningf("[Debug by lz] Trying cleaned txID: %s", cleanTxID)
					fullTxnData, found = c.txnStore.GetByID(cleanTxID)
					if found {
						logger.Warningf("[Debug by lz] Found transaction using cleaned txID: %s", cleanTxID)
						txID = cleanTxID // 更新 txID
					}
				}

				// 3. 如果還是沒找到，列出 store 中的所有 txID 進行比較
				if !found {
					logger.Warningf("[Debug by lz] Still not found. Listing all transactions in store:")
					allTxns := c.txnStore.Get()
					for i, txn := range allTxns {
						logger.Warningf("[Debug by lz] Store txn[%d]: %T", i, txn)
					}
				}
			}

			if found {
				logger.Warningf("[Debug by lz] fullTxnData: %T", fullTxnData)
				logger.Warningf("[Debug by lz] Found full transaction in store for txID: %s", txID)
				// 嘗試從完整交易數據中解析
				if txnBytes, ok := fullTxnData.([]byte); ok {
					// 直接將字節數據反序列化為 Envelope，而不是遞歸調用
					var envelope common.Envelope
					if err := proto.Unmarshal(txnBytes, &envelope); err != nil {
						logger.Warningf("[Debug by lz] Failed to unmarshal transaction envelope: %v", err)
					} else {
						logger.Warningf("[Debug by lz] Successfully unmarshaled envelope from transaction store")
						// 直接使用反序列化的 envelope 繼續處理，並更新 envBytes
						env = &envelope
						envBytes = txnBytes  // 重要：更新 envBytes 為完整的交易字節數據
						goto processEnvelope // 跳轉到處理 envelope 的代碼
					}
				} else if envelope, ok := fullTxnData.(*common.Envelope); ok {
					// 處理當 transaction store 直接返回 *common.Envelope 的情況
					logger.Warningf("[Debug by lz] Found envelope directly in transaction store")
					env = envelope
					// 需要將 envelope 序列化為 bytes 以供後續使用
					if envBytes, err = proto.Marshal(envelope); err != nil {
						logger.Warningf("[Debug by lz] Failed to marshal envelope to bytes: %v", err)
					} else {
						logger.Warningf("[Debug by lz] Successfully marshaled envelope to bytes")
						goto processEnvelope
					}
				} else {
					logger.Warningf("[Debug by lz] Transaction data is not []byte or *common.Envelope type: %T", fullTxnData)
				}
			} else {
				logger.Warningf("[Debug by lz] Transaction not found in store for txID: %s", txID)
			}
		} else {
			logger.Warningf("[Debug by lz] Transaction store is nil")
		}

		// 如果無法從 transaction store 獲取，設置基本信息並返回錯誤
		txInfo.txID = txID
		return nil, err
	}

processEnvelope:
	// Unmarshal the payload
	payload, err := protoutil.UnmarshalPayload(env.Payload)
	if err != nil {
		logger.Warningf("Invalid payload: %s", err)
		return nil, err
	}
	if payload.Header == nil {
		err := errors.New("payload header is nil")
		logger.Warningf("Invalid tx: %s", err)
		return nil, err
	}
	// 取得 channelID 和 txID
	chdr, err := protoutil.UnmarshalChannelHeader(payload.Header.ChannelHeader)
	if err != nil {
		logger.Warningf("Invalid channel header: %s", err)
		return nil, err
	}
	txInfo.channelID = chdr.ChannelId
	txInfo.txID = chdr.TxId

	// 如果不是 ENDORSER_TRANSACTION，就不是你想處理的那類交易，回傳錯誤
	if chdr.Type != int32(common.HeaderType_ENDORSER_TRANSACTION) {
		err := errors.New("header type is not an endorser transaction")
		logger.Debugf("Invalid transaction type: %s", err)
		return nil, err
	}

	// 抽出交易細節（Action, Endorsements, RWSet）
	respPayload, err := protoutil.GetActionFromEnvelope(envBytes)
	if err != nil {
		logger.Warningf("Failed obtaining action from envelope: %s", err)
		return nil, err
	}

	// 取得交易
	tx, err := protoutil.UnmarshalTransaction(payload.Data)
	if err != nil {
		logger.Warningf("Invalid transaction in payload data for tx [%s]: %s", chdr.TxId, err)
		return nil, err
	}

	// 取得 action
	ccActionPayload, err := protoutil.UnmarshalChaincodeActionPayload(tx.Actions[0].Payload)
	if err != nil {
		logger.Warningf("Invalid chaincode action in payload for tx [%s]: %s", chdr.TxId, err)
		return nil, err
	}

	// 取得 action 的 endorsements
	if ccActionPayload.Action == nil {
		logger.Warningf("Action in ChaincodeActionPayload for tx [%s] is nil", chdr.TxId)
		return nil, err
	}
	txInfo.endorsements = ccActionPayload.Action.Endorsements

	// 從交易中解析出 Chaincode 執行後對帳本造成的變更（Read/Write Set，簡稱 RWSet），並把它存進 txInfo.txRWSet
	txRWSet := &rwsetutil.TxRwSet{}
	if err = txRWSet.FromProtoBytes(respPayload.Results); err != nil {
		logger.Warningf("Failed obtaining TxRwSet from ChaincodeAction's results: %s", err)
		return nil, err
	}

	txInfo.txRWSet = txRWSet

	return txInfo, nil
}

// containsWrites checks whether the given CollHashedRwSet contains writes
func containsWrites(txID string, namespace string, colHashedRWSet *rwsetutil.CollHashedRwSet) bool {
	if colHashedRWSet.HashedRwSet == nil {
		logger.Warningf("HashedRWSet of tx [%s], namespace [%s], collection [%s] is nil", txID, namespace, colHashedRWSet.CollectionName)
		return false
	}
	if len(colHashedRWSet.HashedRwSet.HashedWrites) == 0 && len(colHashedRWSet.HashedRwSet.MetadataWrites) == 0 {
		logger.Debugf("HashedRWSet of tx [%s], namespace [%s], collection [%s] doesn't contain writes", txID, namespace, colHashedRWSet.CollectionName)
		return false
	}
	return true
}

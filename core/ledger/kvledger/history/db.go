/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package history

import (
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric/common/ledger/blkstorage"
	"github.com/hyperledger/fabric/common/ledger/dataformat"
	"github.com/hyperledger/fabric/common/ledger/util/leveldbhelper"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/internal/version"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/rwsetutil"
	"github.com/hyperledger/fabric/internal/pkg/txflags"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

var logger = flogging.MustGetLogger("history")

// DBProvider provides handle to HistoryDB for a given channel
type DBProvider struct {
	leveldbProvider *leveldbhelper.Provider
}

// NewDBProvider instantiates DBProvider
func NewDBProvider(path string) (*DBProvider, error) {
	logger.Debugf("constructing HistoryDBProvider dbPath=%s", path)
	levelDBProvider, err := leveldbhelper.NewProvider(
		&leveldbhelper.Conf{
			DBPath:         path,
			ExpectedFormat: dataformat.CurrentFormat,
		},
	)
	if err != nil {
		return nil, err
	}
	return &DBProvider{
		leveldbProvider: levelDBProvider,
	}, nil
}

// MarkStartingSavepoint creates historydb to be used for a ledger that is created from a snapshot
func (p *DBProvider) MarkStartingSavepoint(name string, savepoint *version.Height) error {
	db := p.GetDBHandle(name)
	err := db.levelDB.Put(savePointKey, savepoint.ToBytes(), true)
	return errors.WithMessagef(err, "error while writing the starting save point for ledger [%s]", name)
}

// GetDBHandle gets the handle to a named database
func (p *DBProvider) GetDBHandle(name string) *DB {
	return &DB{
		levelDB: p.leveldbProvider.GetDBHandle(name),
		name:    name,
	}
}

// GetDBHandleWithTxStore gets the handle to a named database with transaction store provider
func (p *DBProvider) GetDBHandleWithTxStore(name string, txStoreProvider func() interface{}) *DB {
	return &DB{
		levelDB:         p.leveldbProvider.GetDBHandle(name),
		name:            name,
		txStoreProvider: txStoreProvider,
	}
}

// Close closes the underlying db
func (p *DBProvider) Close() {
	p.leveldbProvider.Close()
}

// Drop drops channel-specific data from the history db
func (p *DBProvider) Drop(channelName string) error {
	return p.leveldbProvider.Drop(channelName)
}

// DB maintains and provides access to history data for a particular channel
type DB struct {
	levelDB         *leveldbhelper.DBHandle
	name            string
	txStoreProvider func() interface{} // 新增：交易儲存提供者
}

// Commit implements method in HistoryDB interface
func (d *DB) Commit(block *common.Block) error {
	blockNo := block.Header.Number
	// Set the starting tranNo to 0
	var tranNo uint64
	logger.Warningf("[Debug by lz] Commit with DB")
	dbBatch := d.levelDB.NewUpdateBatch()

	logger.Debugf("Channel [%s]: Updating history database for blockNo [%v] with [%d] transactions",
		d.name, blockNo, len(block.Data.Data))

	// Get the invalidation byte array for the block
	txsFilter := txflags.ValidationFlags(block.Metadata.Metadata[common.BlockMetadataIndex_TRANSACTIONS_FILTER])

	// write each tran's write set to history db
	for _, envBytes := range block.Data.Data {

		// If the tran is marked as invalid, skip it
		if txsFilter.IsInvalid(int(tranNo)) {
			logger.Debugf("Channel [%s]: Skipping history write for invalid transaction number %d",
				d.name, tranNo)
			tranNo++
			continue
		}

		// 🔧 嘗試解析為 envelope，如果失敗則從 TransactionStore 查詢
		env, err := protoutil.GetEnvelopeFromBlock(envBytes)
		if err != nil {
			// 假設是 txid，嘗試從 TransactionStore 查詢
			txid := string(envBytes)
			logger.Warningf("[Debug by lz] Failed to parse envelope, trying as txid: %s", txid)

			if d.txStoreProvider != nil {
				txStore := d.txStoreProvider()
				if txStore != nil {
					// 嘗試轉換為 TransactionStore 介面
					if ts, ok := txStore.(interface {
						GetByID(string) (interface{}, bool)
					}); ok {
						if txData, found := ts.GetByID(txid); found {
							// 嘗試轉換為 *common.Envelope
							switch data := txData.(type) {
							case *common.Envelope:
								env = data
								envBytes, err = proto.Marshal(env)
								if err != nil {
									logger.Warningf("[Debug by lz] Failed to marshal envelope for txid %s: %v", txid, err)
									tranNo++
									continue
								}
								logger.Warningf("[Debug by lz] Successfully retrieved envelope from store for txid: %s", txid)
							case []byte:
								// 嘗試解析為 envelope
								envelope := &common.Envelope{}
								if unmarshalErr := proto.Unmarshal(data, envelope); unmarshalErr == nil {
									env = envelope
									envBytes, err = proto.Marshal(env)
									if err != nil {
										logger.Warningf("[Debug by lz] Failed to marshal envelope for txid %s: %v", txid, err)
										tranNo++
										continue
									}
									logger.Warningf("[Debug by lz] Successfully unmarshaled envelope from store for txid: %s", txid)
								} else {
									logger.Warningf("[Debug by lz] Failed to unmarshal envelope for txid %s: %v", txid, unmarshalErr)
									tranNo++
									continue
								}
							default:
								logger.Errorf("Channel [%s]: Unsupported transaction data type %T for txid: %s", d.name, txData, txid)
								tranNo++
								continue
							}
						} else {
							logger.Errorf("Channel [%s]: Transaction not found in store for txid: %s", d.name, txid)
							tranNo++
							continue
						}
					} else {
						logger.Errorf("Channel [%s]: Transaction store does not implement GetByID method", d.name)
						tranNo++
						continue
					}
				} else {
					logger.Errorf("Channel [%s]: Transaction store provider returned nil", d.name)
					tranNo++
					continue
				}
			} else {
				logger.Errorf("Channel [%s]: No transaction store provider available, cannot resolve txid: %s", d.name, txid)
				tranNo++
				continue
			}
		}
		logger.Warningf("[Debug by lz] UnmarshalPayload %v", env)
		logger.Warningf("[Debug by lz] UnmarshalPayload %v", env.Payload)

		payload, err := protoutil.UnmarshalPayload(env.Payload)
		if err != nil {
			return err
		}
		logger.Warningf("[Debug by lz] UnmarshalPayload %v", payload)
		chdr, err := protoutil.UnmarshalChannelHeader(payload.Header.ChannelHeader)
		if err != nil {
			return err
		}
		logger.Warningf("[Debug by lz] UnmarshalChannelHeader %v", chdr)
		if common.HeaderType(chdr.Type) == common.HeaderType_ENDORSER_TRANSACTION {
			// extract RWSet from transaction
			// 這裡的 envBytes 是 txid 的 byte array 是錯誤ㄉ
			// 我該如何拿到完整 envBytes 呢？
			respPayload, err := protoutil.GetActionFromEnvelope(envBytes)
			if err != nil {
				return err
			}
			txRWSet := &rwsetutil.TxRwSet{}
			if err = txRWSet.FromProtoBytes(respPayload.Results); err != nil {
				return err
			}
			// add a history record for each write
			for _, nsRWSet := range txRWSet.NsRwSets {
				ns := nsRWSet.NameSpace

				for _, kvWrite := range nsRWSet.KvRwSet.Writes {
					dataKey := constructDataKey(ns, kvWrite.Key, blockNo, tranNo)
					// No value is required, write an empty byte array (emptyValue) since Put() of nil is not allowed
					dbBatch.Put(dataKey, emptyValue)
				}
			}

		} else {
			logger.Debugf("Skipping transaction [%d] since it is not an endorsement transaction\n", tranNo)
		}
		tranNo++
	}

	// add savepoint for recovery purpose
	height := version.NewHeight(blockNo, tranNo)
	dbBatch.Put(savePointKey, height.ToBytes())

	// write the block's history records and savepoint to LevelDB
	// Setting snyc to true as a precaution, false may be an ok optimization after further testing.
	if err := d.levelDB.WriteBatch(dbBatch, true); err != nil {
		return err
	}

	logger.Debugf("Channel [%s]: Updates committed to history database for blockNo [%v]", d.name, blockNo)
	return nil
}

// NewQueryExecutor implements method in HistoryDB interface
func (d *DB) NewQueryExecutor(blockStore *blkstorage.BlockStore) (ledger.HistoryQueryExecutor, error) {
	return &QueryExecutor{d.levelDB, blockStore}, nil
}

// GetLastSavepoint implements returns the height till which the history is present in the db
func (d *DB) GetLastSavepoint() (*version.Height, error) {
	versionBytes, err := d.levelDB.Get(savePointKey)
	if err != nil || versionBytes == nil {
		return nil, err
	}
	height, _, err := version.NewHeightFromBytes(versionBytes)
	if err != nil {
		return nil, err
	}
	return height, nil
}

// ShouldRecover implements method in interface kvledger.Recoverer
func (d *DB) ShouldRecover(lastAvailableBlock uint64) (bool, uint64, error) {
	savepoint, err := d.GetLastSavepoint()
	if err != nil {
		return false, 0, err
	}
	if savepoint == nil {
		return true, 0, nil
	}
	return savepoint.BlockNum != lastAvailableBlock, savepoint.BlockNum + 1, nil
}

// Name returns the name of the database that manages historical states.
func (d *DB) Name() string {
	return "history"
}

// CommitLostBlock implements method in interface kvledger.Recoverer
func (d *DB) CommitLostBlock(blockAndPvtdata *ledger.BlockAndPvtData) error {
	block := blockAndPvtdata.Block

	// log every 1000th block at Info level so that history rebuild progress can be tracked in production envs.
	if block.Header.Number%1000 == 0 {
		logger.Infof("Recommitting block [%d] to history database", block.Header.Number)
	} else {
		logger.Debugf("Recommitting block [%d] to history database", block.Header.Number)
	}
	return d.Commit(block)
}

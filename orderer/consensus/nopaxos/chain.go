package nopaxos

import (
	"fmt"
	"net"
	"time"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	cb "github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer/etcdraft"
	"github.com/hyperledger/fabric/orderer/common/localconfig"
	"github.com/hyperledger/fabric/orderer/consensus"
	nopaxosConfig "github.com/hyperledger/fabric/orderer/consensus/nopaxos/config"
	"github.com/hyperledger/fabric/orderer/consensus/nopaxos/protocol"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

var logger = flogging.MustGetLogger("orderer.consensus.nopaxos")

type consenter struct {
	config *localconfig.TopLevel
}

type chain struct {
	support       consensus.ConsenterSupport
	sendChan      chan *message
	deliverChan   chan []*message
	exitChan      chan struct{}
	consenters    []*etcdraft.Consenter
	NopaxosServer *Server
	Count         uint64
	batch         [][]byte        // 統一存儲序列化的 envelope bytes
	processedTxs  map[string]bool // 新增：追蹤已處理的交易ID
}

type message struct {
	configSeq       uint64
	normalMsg       *cb.Envelope
	configMsg       *cb.Envelope
	txid            []byte // 用來儲存 txid
	channelID       []byte // 用來儲存 channelID
	hasFullEnvelope bool   // 新增：標記是否有完整的 envelope
}

// New creates a new consenter for the solo consensus scheme.
// The solo consensus scheme is very simple, and allows only one consenter for a given chain (this process).
// It accepts messages being delivered via Order/Configure, orders them, and then uses the blockcutter to form the messages
// into blocks before writing to the given ledger
func New(config *localconfig.TopLevel) consensus.Consenter {
	return &consenter{
		config: config,
	}
}

func (nps *consenter) HandleChain(support consensus.ConsenterSupport, metadata *cb.Metadata) (consensus.Chain, error) {
	m := &etcdraft.ConfigMetadata{}
	if err := proto.Unmarshal(support.SharedConfig().ConsensusMetadata(), m); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal consensus metadata")
	}

	return newChain(support, m.Consenters, nps.config), nil
}

func (nps *consenter) IsChannelMember(joinBlock *cb.Block) (bool, error) {
	return true, nil
}

func newChain(support consensus.ConsenterSupport, consenters []*etcdraft.Consenter, config *localconfig.TopLevel) *chain {

	nopaxosServerConfig := &nopaxosConfig.ProtocolConfig{}

	host, _, err := net.SplitHostPort(config.Operations.ListenAddress)
	if err != nil {
		fmt.Println("Error:", err)
	}

	members := make(map[string]protocol.Member)
	for _, consenter := range consenters {
		if consenter.GetHost() != "orderer4.example.com" {
			members[consenter.GetHost()] = protocol.Member{
				ID:           consenter.GetHost(),
				Host:         consenter.GetHost(),
				APIPort:      int(consenter.GetPort()) + 35,
				ProtocolPort: int(consenter.GetPort()) + 36,
			}
		}
	}

	cluster := protocol.NodeCluster{
		MemberID: host,
		Members:  members,
	}

	deliverChan := make(chan []struct{})

	return &chain{
		support:     support,
		sendChan:    make(chan *message),
		exitChan:    make(chan struct{}),
		deliverChan: make(chan []*message),
		consenters:  consenters,
		NopaxosServer: NewNodeServer(
			cluster,
			nopaxosServerConfig,
			deliverChan,
		),
		Count:        1,
		batch:        make([][]byte, 0),
		processedTxs: make(map[string]bool),
	}
}

func (ch *chain) Start() {
	go ch.main()
}

func (ch *chain) Halt() {
	select {
	case <-ch.exitChan:
		// Allow multiple halts without panic
	default:
		close(ch.exitChan)
	}
}

// TODO: 確認是否需要實作，會不會是因為這個沒有處理造成 approve 有時候會失敗
func (ch *chain) WaitReady() error {
	return nil
}

// Order accepts normal messages for ordering
// func (ch *chain) Order(env *cb.Envelope, configSeq uint64, sequencerId uint64, sequencerNumber uint64) error {
// 	logger.Warningf("[Debug by lz] NOPaxos Order started - sequencerId: %d, sequencerNumber: %d, configSeq: %d", sequencerId, sequencerNumber, configSeq)

// 	// 從 envelope 中提取 payload
// 	logger.Warningf("[Debug by lz] Unmarshaling envelope payload")
// 	payload, err := protoutil.UnmarshalPayload(env.Payload)
// 	if err != nil {
// 		logger.Warningf("[Debug by lz] Error unmarshaling payload: %v", err)
// 		return err
// 	}

// 	// 從 payload header 中提取 channel header
// 	logger.Warningf("[Debug by lz] Unmarshaling channel header")
// 	chdr, err := protoutil.UnmarshalChannelHeader(payload.Header.ChannelHeader)
// 	if err != nil {
// 		logger.Warningf("[Debug by lz] Error unmarshaling channel header: %v", err)
// 		return err
// 	}

// 	// 提取 txid
// 	txid := chdr.TxId
// 	channelID := chdr.ChannelId

// 	logger.Warningf("[Debug by lz] Transaction details - TxID: %s, ChannelID: %s", txid, channelID)
// 	logger.Warningf("[Debug by lz] Converting to txid bytes and calling OrderWithoutVerify")

// 	// 將 txid 轉換為 bytes 並調用 OrderWithoutVerify
// 	return ch.OrderWithoutVerify([]byte(txid), configSeq, sequencerId, sequencerNumber)
// }

// Configure accepts configuration update messages for ordering
// func (ch *chain) Configure(config *cb.Envelope, configSeq uint64) error {
// 	logger.Warningf("[Debug by lz] NOPaxos Configure started - configSeq: %d", configSeq)
// 	logger.Warningf("[Debug by lz] Delegating to ConfigureWithoutVerify")
// 	// 可以簡單地委派給 WithoutVerify 版本
// 	return ch.ConfigureWithoutVerify(nil, configSeq)
// }

// Order accepts normal messages for ordering
func (ch *chain) Order(env *cb.Envelope, configSeq uint64, sequencerId uint64, sequencerNumber uint64) error {
	fmt.Println("=======TESTTEST=======")
	fmt.Println(sequencerNumber)
	fmt.Println("=======Msg Drop Rate=======")
	fmt.Println(float64(ch.Count) / float64(sequencerNumber+1))
	fmt.Println("=======Msg Drop Rate=======")

	msg, err := proto.Marshal(env)
	if err != nil {
		return err
	}

	ch.NopaxosServer.nopaxos.Command(
		&protocol.NewCommandRequest{
			CommandRequest: &protocol.CommandRequest{
				SessionNum: 1,
				MessageNum: protocol.MessageID(sequencerNumber),
				Timestamp:  time.Now(),
				Value:      msg,
			},
			ConfigSeq: configSeq,
		},
		nil,
	)

	ch.Count = ch.Count + 1

	if !ch.NopaxosServer.nopaxos.IsLeader() {
		return nil
	}

	select {
	case ch.sendChan <- &message{
		configSeq:       configSeq,
		normalMsg:       env,
		hasFullEnvelope: true,
	}:
		return nil
	case <-ch.exitChan:
		return fmt.Errorf("Exiting")
	}
}

// 改成只對 txid 做 ordering
func (ch *chain) OrderWithoutVerify(txid []byte, configSeq uint64, sequencerId uint64, sequencerNumber uint64) error {
	logger.Warningf("[Debug by lz] OrderWithoutVerify started - txid: %s, sequencerNumber: %d", string(txid), sequencerNumber)

	dropRate := float64(ch.Count) / float64(sequencerNumber+1)
	logger.Warningf("[Debug by lz] Message statistics - Count: %d, SequencerNumber: %d, Drop Rate: %f", ch.Count, sequencerNumber, dropRate)

	logger.Warningf("[Debug by lz] Creating NOPaxos command request")
	ch.NopaxosServer.nopaxos.Command(
		&protocol.NewCommandRequest{
			CommandRequest: &protocol.CommandRequest{
				SessionNum: 1,
				MessageNum: protocol.MessageID(sequencerNumber),
				Timestamp:  time.Now(),
				Value:      txid,
			},
			ConfigSeq: configSeq,
		},
		nil,
	)
	logger.Warningf("[Debug by lz] Command sent to NOPaxos protocol")

	ch.Count = ch.Count + 1

	if !ch.NopaxosServer.nopaxos.IsLeader() {
		logger.Warningf("[Debug by lz] Not leader, returning without sending to main loop")
		return nil
	}

	logger.Warningf("[Debug by lz] Is leader, sending message to main loop channel")
	select {
	case ch.sendChan <- &message{
		configSeq:       configSeq,
		txid:            txid,
		hasFullEnvelope: false,
	}:
		logger.Warningf("[Debug by lz] Message sent to main loop successfully")
		return nil
	case <-ch.exitChan:
		logger.Warningf("[Debug by lz] Exit signal received, stopping")
		return fmt.Errorf("Exiting")
	}
}

// Configure accepts configuration update messages for ordering
func (ch *chain) Configure(config *cb.Envelope, configSeq uint64) error {
	select {
	case ch.sendChan <- &message{
		configSeq: configSeq,
		configMsg: config,
	}:
		return nil
	case <-ch.exitChan:
		return fmt.Errorf("Exiting")
	}
}

func (ch *chain) ConfigureWithoutVerify(txid []byte, configSeq uint64) error {
	logger.Warningf("[Debug by lz] ConfigureWithoutVerify started - txid: %s, configSeq: %d", string(txid), configSeq)
	logger.Warningf("[Debug by lz] Sending config message to main loop")
	select {
	case ch.sendChan <- &message{
		configSeq: configSeq,
		txid:      txid,
	}:
		logger.Warningf("[Debug by lz] Config message sent to main loop successfully")
		return nil
	case <-ch.exitChan:
		logger.Warningf("[Debug by lz] Exit signal received during config")
		return fmt.Errorf("Exiting")
	}
}

// Errored only closes on exit
func (ch *chain) Errored() <-chan struct{} {
	return ch.exitChan
}

func (ch *chain) main() {
	var timer <-chan time.Time
	var err error

	go ch.NopaxosServer.Start()
	defer ch.NopaxosServer.Stop()

	for {
		logger.Warningf("[Debug by lz] NOPaxos main loop iteration - waiting for messages")
		seq := ch.support.Sequence()
		err = nil
		select {
		case msg := <-ch.sendChan:
			logger.Warningf("[Debug by lz] Received message in main loop - configSeq: %d, current seq: %d, txid: %s", msg.configSeq, seq, string(msg.txid))
			// 如果 msg 有 txid，則是 normalMsg

			if msg.txid != nil {
				logger.Warningf("[Debug by lz] Processing normal txid message")
				// TxnMsg
				if msg.configSeq < seq {
					logger.Warningf("[Debug by lz] ConfigSeq (%d) < current seq (%d), calling ProcessNormalMsgWithoutVerify", msg.configSeq, seq)
					_, err = ch.support.ProcessNormalMsgWithoutVerify()
					if err != nil {
						logger.Warningf("[Debug by lz] ProcessNormalMsgWithoutVerify failed: %s", err)
						continue
					}
					logger.Warningf("[Debug by lz] ProcessNormalMsgWithoutVerify succeeded")
				}

				// 檢查是否已經處理過這個 txid
				txidStr := string(msg.txid)
				if ch.processedTxs[txidStr] {
					logger.Warningf("[Debug by lz] Duplicate txid detected, skipping: %s", txidStr)
					continue
				}
				logger.Warningf("[Debug by lz] txid: %s", string(msg.txid))
				logger.Warningf("[Debug by lz] ch.batch before append: %v, batch size: %d", ch.batch, len(ch.batch))

				txidCopy := make([]byte, len(msg.txid))
				copy(txidCopy, msg.txid)
				ch.batch = append(ch.batch, txidCopy)
				
				ch.processedTxs[txidStr] = true
				logger.Warningf("[Debug by lz] ch.batch after append: %v, batch size: %d", ch.batch, len(ch.batch))

				if len(ch.batch) > 512 {
					logger.Warningf("[Debug by lz] Batch size exceeded 512 (%d), creating block", len(ch.batch))
					block := ch.support.CreateNextBlockWithoutVerify(ch.batch)
					ch.support.WriteBlock(block, nil)
					logger.Warningf("[Debug by lz] Block written successfully, clearing batch")
					ch.batch = [][]byte{}
					// TODO: 這裡有需要防止 race condition 嗎？
					ch.processedTxs = make(map[string]bool) // 清空已處理交易記錄
					if timer != nil {
						timer = nil
					}
				}

				pending := len(ch.batch) > 0
				logger.Warningf("[Debug by lz] Checking timer state - pending: %v, timer running: %v", pending, timer != nil)

				switch {
				case timer != nil && !pending:
					// Timer is already running but there are no messages pending, stop the timer
					logger.Warningf("[Debug by lz] Stopping timer - no pending messages")
					timer = nil
				case timer == nil && pending:
					// Timer is not already running and there are messages pending, so start it
					logger.Warningf("[Debug by lz] Starting 250ms batch timer - %d messages pending", len(ch.batch))
					timer = time.After(250 * time.Millisecond)
					logger.Debugf("Just began %s batch timer", ch.support.SharedConfig().BatchTimeout().String())
				default:
					// Do nothing when:
					// 1. Timer is already running and there are messages pending
					// 2. Timer is not set and there are no messages pending
					logger.Warningf("[Debug by lz] No timer action needed")
				}
			} else if msg.configMsg != nil {
				// ConfigMsg
				// 目的是為了確保 config 的 seq 不會超過目前的 seq
				logger.Warningf("[Debug by lz] Processing config message")
				if msg.configSeq < seq {
					// 不處理 config 的 msg
					msg.configMsg, _, err = ch.support.ProcessConfigMsg(msg.configMsg)
					if err != nil {
						logger.Warningf("Discarding bad config message: %s", err)
						continue
					}
				}
				batch := ch.support.BlockCutter().Cut()
				if batch != nil {
					block := ch.support.CreateNextBlock(batch)
					ch.support.WriteBlock(block, nil)
				}

				block := ch.support.CreateNextBlock([]*cb.Envelope{msg.configMsg})
				ch.support.WriteConfigBlock(block, nil)
				timer = nil

			} else if msg.hasFullEnvelope {
				// approve transaction message
				logger.Warningf("[Debug by lz] Processing approve transaction message")

				// 先從 envelope 中提取 txid 檢查重複
				payload, err := protoutil.UnmarshalPayload(msg.normalMsg.Payload)
				if err != nil {
					logger.Warningf("[Debug by lz] Error unmarshaling payload: %v", err)
					continue
				}

				chdr, err := protoutil.UnmarshalChannelHeader(payload.Header.ChannelHeader)
				if err != nil {
					logger.Warningf("[Debug by lz] Error unmarshaling channel header: %v", err)
					continue
				}

				txidStr := chdr.TxId
				logger.Warningf("[Debug by lz] Extracted txid from envelope: %s", txidStr)

				// 檢查是否已經處理過這個 txid
				if ch.processedTxs[txidStr] {
					logger.Warningf("[Debug by lz] Duplicate txid detected (from envelope), skipping: %s", txidStr)
					continue
				}

				msgBytes, err := proto.Marshal(msg.normalMsg)
				if err != nil {
					logger.Warningf("[Debug by lz] Failed to marshal envelope: %v", err)
					continue
				}

				logger.Warningf("[Debug by lz] Adding envelope to batch - current batch size: %d", len(ch.batch))
				ch.batch = append(ch.batch, msgBytes)
				ch.processedTxs[txidStr] = true
				logger.Warningf("[Debug by lz] Added envelope txid to processed set: %s", txidStr)

				if len(ch.batch) > 512 {
					logger.Warningf("[Debug by lz] Batch size exceeded 512 (%d), creating block", len(ch.batch))
					block := ch.support.CreateNextBlockWithoutVerify(ch.batch)
					ch.support.WriteBlock(block, nil)
					logger.Warningf("[Debug by lz] Block written successfully, clearing batch")
					ch.batch = [][]byte{}
					ch.processedTxs = make(map[string]bool) // 清空已處理交易記錄
					if timer != nil {
						timer = nil
					}
				}

				pending := len(ch.batch) > 0
				logger.Warningf("[Debug by lz] Checking timer state - pending: %v, timer running: %v", pending, timer != nil)

				switch {
				case timer != nil && !pending:
					// Timer is already running but there are no messages pending, stop the timer
					logger.Warningf("[Debug by lz] Stopping timer - no pending messages")
					timer = nil
				case timer == nil && pending:
					// Timer is not already running and there are messages pending, so start it
					logger.Warningf("[Debug by lz] Starting 250ms batch timer - %d messages pending", len(ch.batch))
					timer = time.After(250 * time.Millisecond)
					logger.Debugf("Just began %s batch timer", ch.support.SharedConfig().BatchTimeout().String())
				default:
					// Do nothing when:
					// 1. Timer is already running and there are messages pending
					// 2. Timer is not set and there are no messages pending
					logger.Warningf("[Debug by lz] No timer action needed")
				}
			} else {
				logger.Warningf("[Debug by lz] Received unknown message type")
				continue
			}
		case <-timer:
			//clear the timer
			// Timer timeout：代表需要基於目前 batch 切 block
			logger.Warningf("[Debug by lz] Batch timer expired - processing pending transactions")
			timer = nil

			if len(ch.batch) == 0 {
				logger.Warningf("[Debug by lz] Timer expired but no pending transactions - possible bug")
				continue
			}
			logger.Warningf("[Debug by lz] Creating block from %d pending transactions", len(ch.batch))
			// 根據 batch 寫進入 block
			// 為什麼這裡就已經出現重複的 txid
			// ch.batch: [[100 51 48 51 53 55 98 54 101 56 102 53 56 54 51 48 100 55 49 53 51 57 54 50 53 56 54 49 50 101 97 55 49 101 101 49 98 57 53 51 97 49 51 48 98 52 57 101 102 56 48 100 52 99 51 101 98 49 100 49 99 52 56 97] [100 51 48 51 53 55 98 54 101 56 102 53 56 54 51 48 100 55 49 53 51 57 54 50 53 56 54 49 50 101 97 55 49 101 101 49 98 57 53 51 97 49 51 48 98 52 57 101 102 56 48 100 52 99 51 101 98 49 100 49 99 52 56 97] [100 51 48 51 53 55 98 54 101 56 102 53 56 54 51 48 100 55 49 53 51 57 54 50 53 56 54 49 50 101 97 55 49 101 101 49 98 57 53 51 97 49 51 48 98 52 57 101 102 56 48 100 52 99 51 101 98 49 100 49 99 52 56 97] [100 51 48 51 53 55 98 54 101 56 102 53 56 54 51 48 100 55 49 53 51 57 54 50 53 56 54 49 50 101 97 55 49 101 101 49 98 57 53 51 97 49 51 48 98 52 57 101 102 56 48 100 52 99 51 101 98 49 100 49 99 52 56 97] [100 51 48 51 53 55 98 54 101 56 102 53 56 54 51 48 100 55 49 53 51 57 54 50 53 56 54 49 50 101 97 55 49 101 101 49 98 57 53 51 97 49 51 48 98 52 57 101 102 56 48 100 52 99 51 101 98 49 100 49 99 52 56 97] [100 51 48 51 53 55 98 54 101 56 102 53 56 54 51 48 100 55 49 53 51 57 54 50 53 56 54 49 50 101 97 55 49 101 101 49 98 57 53 51 97 49 51 48 98 52 57 101 102 56 48 100 52 99 51 101 98 49 100 49 99 52 56 97] [100 51 48 51 53 55 98 54 101 56 102 53 56 54 51 48 100 55 49 53 51 57 54 50 53 56 54 49 50 101 97 55 49 101 101 49 98 57 53 51 97 49 51 48 98 52 57 101 102 56 48 100 52 99 51 101 98 49 100 49 99 52 56 97] [100 51 48 51 53 55 98 54 101 56 102 53 56 54 51 48 100 55 49 53 51 57 54 50 53 56 54 49 50 101 97 55 49 101 101 49 98 57 53 51 97 49 51 48 98 52 57 101 102 56 48 100 52 99 51 101 98 49 100 49 99 52 56 97] [100 51 48 51 53 55 98 54 101 56 102 53 56 54 51 48 100 55 49 53 51 57 54 50 53 56 54 49 50 101 97 55 49 101 101 49 98 57 53 51 97 49 51 48 98 52 57 101 102 56 48 100 52 99 51 101 98 49 100 49 99 52 56 97] [100 51 48 51 53 55 98 54 101 56 102 53 56 54 51 48 100 55 49 53 51 57 54 50 53 56 54 49 50 101 97 55 49 101 101 49 98 57 53 51 97 49 51 48 98 52 57 101 102 56 48 100 52 99 51 101 98 49 100 49 99 52 56 97] [100 51 48 51 53 55 98 54 101 56 102 53 56 54 51 48 100 55 49 53 51 57 54 50 53 56 54 49 50 101 97 55 49 101 101 49 98 57 53 51 97 49 51 48 98 52 57 101 102 56 48 100 52 99 51 101 98 49 100 49 99 52 56 97] [100 51 48 51 53 55 98 54 101 56 102 53 56 54 51 48 100 55 49 53 51 57 54 50 53 56 54 49 50 101 97 55 49 101 101 49 98 57 53 51 97 49 51 48 98 52 57 101 102 56 48 100 52 99 51 101 98 49 100 49 99 52 56 97] [100 51 48 51 53 55 98 54 101 56 102 53 56 54 51 48 100 55 49 53 51 57 54 50 53 56 54 49 50 101 97 55 49 101 101 49 98 57 53 51 97 49 51 48 98 52 57 101 102 56 48 100 52 99 51 101 98 49 100 49 99 52 56 97] [100 51 48 51 53 55 98 54 101 56 102 53 56 54 51 48 100 55 49 53 51 57 54 50 53 56 54 49 50 101 97 55 49 101 101 49 98 57 53 51 97 49 51 48 98 52 57 101 102 56 48 100 52 99 51 101 98 49 100 49 99 52 56 97] [100 51 48 51 53 55 98 54 101 56 102 53 56 54 51 48 100 55 49 53 51 57 54 50 53 56 54 49 50 101 97 55 49 101 101 49 98 57 53 51 97 49 51 48 98 52 57 101 102 56 48 100 52 99 51 101 98 49 100 49 99 52 56 97] [100 51 48 51 53 55 98 54 101 56 102 53 56 54 51 48 100 55 49 53 51 57 54 50 53 56 54 49 50 101 97 55 49 101 101 49 98 57 53 51 97 49 51 48 98 52 57 101 102 56 48 100 52 99 51 101 98 49 100 49 99 52 56 97] [100 51 48 51 53 55 98 54 101 56 102 53 56 54 51 48 100 55 49 53 51 57 54 50 53 56 54 49 50 101 97 55 49 101 101 49 98 57 53 51 97 49 51 48 98 52 57 101 102 56 48 100 52 99 51 101 98 49 100 49 99 52 56 97] [100 51 48 51 53 55 98 54 101 56 102 53 56 54 51 48 100 55 49 53 51 57 54 50 53 56 54 49 50 101 97 55 49 101 101 49 98 57 53 51 97 49 51 48 98 52 57 101 102 56 48 100 52 99 51 101 98 49 100 49 99 52 56 97] [100 51 48 51 53 55 98 54 101 56 102 53 56 54 51 48 100 55 49 53 51 57 54 50 53 56 54 49 50 101 97 55 49 101 101 49 98 57 53 51 97 49 51 48 98 52 57 101 102 56 48 100 52 99 51 101 98 49 100 49 99 52 56 97] [100 51 48 51 53 55 98 54 101 56 102 53 56 54 51 48 100 55 49 53 51 57 54 50 53 56 54 49 50 101 97 55 49 101 101 49 98 57 53 51 97 49 51 48 98 52 57 101 102 56 48 100 52 99 51 101 98 49 100 49 99 52 56 97] [100 51 48 51 53 55 98 54 101 56 102 53 56 54 51 48 100 55 49 53 51 57 54 50 53 56 54 49 50 101 97 55 49 101 101 49 98 57 53 51 97 49 51 48 98 52 57 101 102 56 48 100 52 99 51 101 98 49 100 49 99 52 56 97] [100 51 48 51 53 55 98 54 101 56 102 53 56 54 51 48 100 55 49 53 51 57 54 50 53 56 54 49 50 101 97 55 49 101 101 49 98 57 53 51 97 49 51 48 98 52 57 101 102 56 48 100 52 99 51 101 98 49 100 49 99 52 56 97] [100 51 48 51 53 55 98 54 101 56 102 53 56 54 51 48 100 55 49 53 51 57 54 50 53 56 54 49 50 101 97 55 49 101 101 49 98 57 53 51 97 49 51 48 98 52 57 101 102 56 48 100 52 99 51 101 98 49 100 49 99 52 56 97]]
			block := ch.support.CreateNextBlockWithoutVerify(ch.batch)
			ch.support.WriteBlock(block, nil)
			logger.Warningf("[Debug by lz] Block created and written successfully, clearing batch")
			ch.batch = [][]byte{}
			// 這裡有需要防止 race condition 嗎？
			ch.processedTxs = make(map[string]bool) // 清空已處理交易記錄

		case <-ch.exitChan:
			logger.Warningf("[Debug by lz] Exit signal received, shutting down NOPaxos main loop")
			return
		}
	}
}

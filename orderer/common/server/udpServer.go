package server

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"strconv"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric/orderer/common/broadcast"
	"github.com/hyperledger/fabric/orderer/common/multichannel"
	"google.golang.org/protobuf/proto"
)

type UdpServer struct {
	host string
	port uint16
	*multichannel.Registrar
	exitChanUDP chan struct{}
}

func NewUDPServer(
	_host string,
	_port uint16,
	r *multichannel.Registrar,
) *UdpServer {
	return &UdpServer{host: _host, port: _port, exitChanUDP: make(chan struct{}), Registrar: r}
}

func (us *UdpServer) Start() error {
	address := net.JoinHostPort("", strconv.Itoa(int(us.port)))
	addr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		fmt.Println("Error resolving address:", err)
		return err
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		fmt.Println("Error listening:", err)
		return err
	}

	buffer := make([]byte, 10240)
	for {
		select {
		case <-us.exitChanUDP:
			conn.Close()
			return nil
		default:
			if err := us.handleUDPMessage(conn, buffer); err != nil {
				fmt.Printf("Error handling UDP message: %v\n", err)
				continue
			}
		}
	}
}

// handleUDPMessage processes a single UDP message
func (us *UdpServer) handleUDPMessage(conn *net.UDPConn, buffer []byte) error {
	n, clientAddr, err := conn.ReadFromUDP(buffer)
	if err != nil {
		return fmt.Errorf("error reading from connection: %w", err)
	}

	if n < 2 {
		return fmt.Errorf("not enough data received: %d bytes", n)
	}

	// Determine message type based on size
	if n > 1024 {
		return us.handleApproveTransaction(buffer, n, clientAddr)
	}

	return us.handleNormalMessage(buffer, n, clientAddr)
}

// handleApproveTransaction processes approve transaction messages (> 1024 bytes)
func (us *UdpServer) handleApproveTransaction(buffer []byte, n int, clientAddr *net.UDPAddr) error {
	fmt.Println("[Debug by lz] Approve transaction received")

	// Extract sequence bytes from the end of the message
	seqBytes, err := us.extractSequenceBytes(buffer, n)
	if err != nil {
		return fmt.Errorf("failed to extract sequence bytes: %w", err)
	}

	// Unmarshal the envelope (excluding front reserve and sequence bytes)
	envelope, err := us.unmarshalEnvelope(buffer[2 : n-4])
	if err != nil {
		return fmt.Errorf("failed to unmarshal envelope: %w", err)
	}

	return us.processEnvelopeMessage(envelope, seqBytes, clientAddr)
}

// handleNormalMessage processes normal messages (< 1024 bytes)
func (us *UdpServer) handleNormalMessage(buffer []byte, n int, clientAddr *net.UDPAddr) error {
	// Parse message components
	txidBytes := buffer[2:66]
	channelIDBytes := buffer[66 : n-4]
	channelID := string(channelIDBytes)

	seqBytes, err := us.extractSequenceBytes(buffer, n)
	if err != nil {
		return fmt.Errorf("failed to extract sequence bytes: %w", err)
	}

	us.logNormalMessageDetails(txidBytes, channelIDBytes, seqBytes, buffer)

	processor := us.BroadcastChannelSupportWithoutVerify(channelID)

	return us.processNormalMessage(processor, txidBytes, channelID, seqBytes, clientAddr)
}

// extractSequenceBytes extracts and converts sequence bytes to uint64
func (us *UdpServer) extractSequenceBytes(buffer []byte, n int) (uint64, error) {
	seqBytes := buffer[n-4 : n]

	paddedBytes := make([]byte, 8)
	copy(paddedBytes[:8-len(seqBytes)], seqBytes)

	var bigEndianValue uint64
	err := binary.Read(bytes.NewReader(paddedBytes), binary.LittleEndian, &bigEndianValue)
	if err != nil {
		return 0, fmt.Errorf("error decoding sequence bytes: %w", err)
	}

	fmt.Printf("Sequence value (uint64): %d (0x%x)\n", bigEndianValue, bigEndianValue)
	return bigEndianValue, nil
}

// unmarshalEnvelope unmarshals protobuf envelope
func (us *UdpServer) unmarshalEnvelope(data []byte) (*common.Envelope, error) {
	envelope := &common.Envelope{}
	err := proto.Unmarshal(data, envelope)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal envelope: %w", err)
	}
	return envelope, nil
}

// logNormalMessageDetails logs details of normal messages for debugging
func (us *UdpServer) logNormalMessageDetails(txidBytes, channelIDBytes []byte, seqNum uint64, buffer []byte) {
	fmt.Printf("Received buffer: %x\n", buffer)
	fmt.Printf("Received txid: %s\n", string(txidBytes))
	fmt.Printf("Received channelID: %s\n", string(channelIDBytes))
	fmt.Printf("Received seqBytes: %d\n", seqNum)
}

// processEnvelopeMessage processes envelope-based messages (approve transactions)
func (us *UdpServer) processEnvelopeMessage(envelope *common.Envelope, seqNum uint64, clientAddr *net.UDPAddr) error {
	chdr, isConfig, processor, err := us.BroadcastChannelSupport(envelope)
	if err != nil {
		return fmt.Errorf("failed to get broadcast channel support: %w", err)
	}

	if !isConfig {
		return us.processNormalEnvelopeMessage(chdr, processor, envelope, seqNum, clientAddr)
	}

	return us.processConfigEnvelopeMessage(chdr, processor, envelope, seqNum, clientAddr)
}

// processNormalEnvelopeMessage processes normal envelope messages
func (us *UdpServer) processNormalEnvelopeMessage(chdr *common.ChannelHeader, processor broadcast.ChannelSupport, envelope *common.Envelope, seqNum uint64, clientAddr *net.UDPAddr) error {
	logger.Debugf("[channel: %s] Broadcast is processing normal message from %s with txid '%s'", chdr.ChannelId, clientAddr, chdr.TxId)

	configSeq, err := processor.ProcessNormalMsg(envelope)
	if err != nil {
		logger.Warningf("[channel: %s] Rejecting broadcast of normal message from %s because of error: %s", chdr.ChannelId, clientAddr, err)
		return err
	}

	if err = processor.WaitReady(); err != nil {
		logger.Warningf("[channel: %s] Rejecting broadcast of message from %s with SERVICE_UNAVAILABLE: rejected by Consenter: %s", chdr.ChannelId, clientAddr, err)
		return err
	}

	err = processor.Order(envelope, configSeq, 1, seqNum)
	if err != nil {
		logger.Warningf("[channel: %s] Rejecting broadcast of normal message from %s with SERVICE_UNAVAILABLE: rejected by Order: %s", chdr.ChannelId, clientAddr, err)
		return err
	}

	return nil
}

// processConfigEnvelopeMessage processes config envelope messages
func (us *UdpServer) processConfigEnvelopeMessage(chdr *common.ChannelHeader, processor broadcast.ChannelSupport, envelope *common.Envelope, seqNum uint64, clientAddr *net.UDPAddr) error {
	logger.Debugf("[channel: %s] Broadcast is processing config update message from %s", chdr.ChannelId, clientAddr)

	config, configSeq, err := processor.ProcessConfigUpdateMsg(envelope)
	if err != nil {
		logger.Warningf("[channel: %s] Rejecting broadcast of config message from %s because of error: %s", chdr.ChannelId, clientAddr, err)
		return err
	}

	if err = processor.WaitReady(); err != nil {
		logger.Warningf("[channel: %s] Rejecting broadcast of message from %s with SERVICE_UNAVAILABLE: rejected by Consenter: %s", chdr.ChannelId, clientAddr, err)
		return err
	}

	err = processor.Configure(config, configSeq)
	if err != nil {
		logger.Warningf("[channel: %s] Rejecting broadcast of config message from %s with SERVICE_UNAVAILABLE: rejected by Configure: %s", chdr.ChannelId, clientAddr, err)
		return err
	}

	return nil
}

// processNormalMessage processes normal messages without envelope
func (us *UdpServer) processNormalMessage(processor *multichannel.ChainSupport, txidBytes []byte, channelID string, seqNum uint64, clientAddr *net.UDPAddr) error {
	logger.Debugf("[channel: %s] Broadcast is processing normal message from %s with txid '%s'", channelID, clientAddr, string(txidBytes))

	configSeq, err := processor.ProcessNormalMsgWithoutVerify()
	if err != nil {
		logger.Warningf("[channel: %s] Rejecting broadcast of message from %s with SERVICE_UNAVAILABLE: rejected by ProcessNormalMsgWithoutVerify: %s", channelID, clientAddr, err)
		return err
	}

	if err = processor.WaitReady(); err != nil {
		logger.Warningf("[channel: %s] Rejecting broadcast of message from %s with SERVICE_UNAVAILABLE: rejected by Consenter: %s", channelID, clientAddr, err)
		return err
	}

	err = processor.OrderWithoutVerify(txidBytes, configSeq, 1, seqNum)
	if err != nil {
		logger.Warningf("[channel: %s] Rejecting broadcast of normal message from %s with SERVICE_UNAVAILABLE: rejected by Order: %s", channelID, clientAddr, err)
		return err
	}

	return nil
}

func (s *UdpServer) Close() {
	// Implementation to close the UDP server
	fmt.Println("Closing UDP server on", s.host, ":", s.port)
	// Logic to clean up resources goes here
	close(s.exitChanUDP) // Signal server exit, if needed
}

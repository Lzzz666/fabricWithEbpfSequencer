package server

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"strconv"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
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

	buffer := make([]byte, 128)
	for {
		select {
		case <-us.exitChanUDP:
			conn.Close()
			return nil
		default:
			// Read UDP data
			n, _, err := conn.ReadFromUDP(buffer)
			if err != nil {
				fmt.Println("Error reading from connection:", err)
				continue
			}

			// Extract the extra bytes from the tail
			if n < 2 {
				fmt.Println("Not enough data received")
				continue
			}

			txidBytes := buffer[2:66]
			channelIDBytes := buffer[66 : n-4]
			SeqNum := binary.LittleEndian.Uint32(buffer[n-4 : n]) // The last 2 bytes are the extra bytes
			fmt.Printf("Received buffer: %x\n", buffer)
			fmt.Printf("Received txid: %s\n", string(txidBytes))
			fmt.Printf("Received channelID: %s\n", string(channelIDBytes))
			fmt.Printf("Received seqBytes: %d\n", SeqNum)
			seqBytes := buffer[n-4 : n]
			fmt.Printf("Received extra bytes: %x\n", seqBytes)

			paddedBytes := make([]byte, 8)
			copy(paddedBytes[:8-len(seqBytes)], seqBytes)
			var bigEndianValue uint64
			err = binary.Read(bytes.NewReader(paddedBytes), binary.LittleEndian, &bigEndianValue)
			if err != nil {
				fmt.Println("Error decoding Big Endian value:", err)
			}
			fmt.Printf("Big Endian interpreted value (uint64): %d (0x%x)\n", bigEndianValue, bigEndianValue)

			envelope := &common.Envelope{}

			err = proto.Unmarshal(buffer[2:n-4], envelope)
			if err != nil {
				fmt.Println("Failed to unmarshal envelope:", err)
				fmt.Println("common/server")
				continue
			}

			// fail here -> msg 要包含 channelID 在裡面
			chdr, isConfig, processor, err := us.BroadcastChannelSupport(envelope)
			if err != nil {
				fmt.Println("Failed to broadcast channel support:", err)
				continue
			}

			if !isConfig {
				logger.Debugf("[channel: %s] Broadcast is processing normal message from %s with txid '%s'", chdr.ChannelId, addr, chdr.TxId)

				configSeq, err := processor.ProcessNormalMsg(envelope)
				if err != nil {
					logger.Warningf("[channel: %s] Rejecting broadcast of normal message from %s because of error: %s", chdr.ChannelId, addr, err)
					continue
				}

				if err = processor.WaitReady(); err != nil {
					logger.Warningf("[channel: %s] Rejecting broadcast of message from %s with SERVICE_UNAVAILABLE: rejected by Consenter: %s", chdr.ChannelId, addr, err)
					continue
				}

				err = processor.Order(envelope, configSeq, 1, bigEndianValue)
				if err != nil {
					logger.Warningf("[channel: %s] Rejecting broadcast of normal message from %s with SERVICE_UNAVAILABLE: rejected by Order: %s", chdr.ChannelId, addr, err)
					continue
				}
			} else { // isConfig
				logger.Debugf("[channel: %s] Broadcast is processing config update message from %s", chdr.ChannelId, addr)

				config, configSeq, err := processor.ProcessConfigUpdateMsg(envelope)
				if err != nil {
					logger.Warningf("[channel: %s] Rejecting broadcast of config message from %s because of error: %s", chdr.ChannelId, addr, err)
					continue
				}

				if err = processor.WaitReady(); err != nil {
					logger.Warningf("[channel: %s] Rejecting broadcast of message from %s with SERVICE_UNAVAILABLE: rejected by Consenter: %s", chdr.ChannelId, addr, err)
					continue
				}

				err = processor.Configure(config, configSeq)
				if err != nil {
					logger.Warningf("[channel: %s] Rejecting broadcast of config message from %s with SERVICE_UNAVAILABLE: rejected by Configure: %s", chdr.ChannelId, addr, err)
					continue
				}
			}
		}
	}
}

func (s *UdpServer) Close() {
	// Implementation to close the UDP server
	fmt.Println("Closing UDP server on", s.host, ":", s.port)
	// Logic to clean up resources goes here
	close(s.exitChanUDP) // Signal server exit, if needed
}

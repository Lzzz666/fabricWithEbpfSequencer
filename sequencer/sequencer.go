package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
)

var wgg sync.WaitGroup

// This is the sequencer that will receive the "transaction hash" from the client and broadcast it to the orderers

func main() {
	param1 := os.Args[1] // First argument (should be an integer)
	broadcastCount, _ := strconv.Atoi(param1)

	addr, err := net.ResolveUDPAddr("udp4", "0.0.0.0:7072")
	if err != nil {
		fmt.Println("Error resolving address:", err)
		return
	}

	conn, err := net.ListenUDP("udp4", addr)
	if err != nil {
		fmt.Println("Error listening:", err)
		return
	}
	defer conn.Close()

	var count uint32 = 0

	buffer := make([]byte, 10240)

	defer conn.Close()

	for {
		// Read UDP data
		n, _, err := conn.ReadFromUDP(buffer)
		if err != nil {
			fmt.Println("Error reading from connection:", err)
			continue
		}
		fmt.Printf("[Sequencer Debug] Received %d bytes: %s\n", n, buffer[:n])
		
		if n > 1024 {
			fmt.Println("[Debug by lz] Approve transaction received")
		}
		
		if n < 2 {
			fmt.Println("Not enough data received")
			continue
		}
		
		// Extract the extra bytes from the tail

		seqBytes := make([]byte, 4) // The extra bytes you want to add
		binary.LittleEndian.PutUint32(seqBytes, count)
		dataWithseqBytes := append(buffer[:n-4], seqBytes...)

		fmt.Println("=====MSG COUNT=====")
		fmt.Println(count)

		ports := [9]string{"3073", "4073", "5073", "6073", "7073", "9073", "10073", "8073"}
		// addrs := [9]string{"192.168.50.232", "192.168.50.123", "192.168.50.137", "192.168.50.188", "192.168.50.239", "192.168.50.219", "192.168.50.182", "192.168.50.188", "192.168.50.230"}
		addrs := [9]string{"localhost", "localhost", "localhost", "localhost", "localhost", "localhost", "localhost", "localhost"}
		for i := 8 - broadcastCount; i < 8; i++ {
			ordererAddress := net.JoinHostPort(addrs[i], ports[i])
			fmt.Printf("[Sequencer Debug] Broadcasting to orderer address: %s\n", ordererAddress)
			ordererServerAddr, err := net.ResolveUDPAddr("udp", ordererAddress)
			if err != nil {
				fmt.Println("Error resolving address:", err)
				continue
			}

			ordererConn, err := net.DialUDP("udp", nil, ordererServerAddr)
			if err != nil {
				fmt.Println("Error connecting to server:", err)
			}
			fmt.Printf("[Sequencer Debug] Forwarding data: %x\n", dataWithseqBytes)
			err = forward(dataWithseqBytes, ordererConn) // Use local err to avoid data race
			if err != nil {
				fmt.Println("Error forward to orderer:", err)
			}
		}
		fmt.Println("[Sequencer Debug] Successful broadcast, incrementing count")

		count++

		// Optionally, respond to the client
	}
}

func forward(tx []byte, conn *net.UDPConn) error {
	_, err := conn.Write(tx)
	if err != nil {
		fmt.Println("Error sending envelope with extra bytes:", err)
		return err
	}

	return nil
}

func parseTxidAndChannel(data string) (string, string, error) {
	// TxID 應該是 66 個十六進制字符 (32 bytes * 2)
	const txidLength = 66

	if len(data) <= txidLength {
		return "", "", fmt.Errorf("data too short, expected at least %d characters", txidLength+1)
	}

	// 檢查前 66 個字符是否都是有效的十六進制
	txidHex := data[:txidLength]
	txid := txidHex
	channelID := data[txidLength:]

	return txid, channelID, nil
}

// 檢查是否為有效的十六進制字符串

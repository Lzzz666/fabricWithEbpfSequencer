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

	var count uint32 = 1

	buffer := make([]byte, 10240)

	defer conn.Close()

	for {
		// Read UDP data
		n, _, err := conn.ReadFromUDP(buffer)
		if err != nil {
			fmt.Println("Error reading from connection:", err)
			continue
		}
		fmt.Printf("Received %d bytes: %s\n", n, buffer[:n])
		fmt.Printf("Received %d bytes: %s\n", n, buffer)
		// Extract the extra bytes from the tail
		if n < 2 {
			fmt.Println("Not enough data received")
			continue
		}
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
			fmt.Println("broadcast to orderer address:", ordererAddress)
			ordererServerAddr, err := net.ResolveUDPAddr("udp", ordererAddress)
			if err != nil {
				fmt.Println("Error resolving address:", err)
				continue
			}

			ordererConn, err := net.DialUDP("udp", nil, ordererServerAddr)
			if err != nil {
				fmt.Println("Error connecting to server:", err)
			}
			fmt.Println("dataWithseqBytes:", dataWithseqBytes)
			err = forward(dataWithseqBytes, ordererConn) // Use local err to avoid data race
			if err != nil {
				fmt.Println("Error forward to orderer:", err)
			}
		}
		fmt.Println("Successful broadcast")
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

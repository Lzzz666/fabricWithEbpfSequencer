// Copyright 2019-present Open Networking Foundation.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package nopaxos

import (
	"fmt"
	"net"
	"sync"

	"github.com/hyperledger/fabric/orderer/consensus/nopaxos/config"
	nopaxos "github.com/hyperledger/fabric/orderer/consensus/nopaxos/protocol"
	"google.golang.org/grpc"
)

// NewServer returns a new NOPaxos consensus protocol server
func NewNodeServer(clusterConfig nopaxos.NodeCluster, protocolConfig *config.ProtocolConfig, deliverChan chan []struct{}) *Server {
	member, ok := clusterConfig.Members[clusterConfig.MemberID]
	if !ok {
		panic("Local member is not present in cluster configuration!")
	}

	cluster := nopaxos.NewCluster(clusterConfig)
	nopaxos := nopaxos.NewNOPaxos(cluster, protocolConfig, deliverChan)
	server := &Server{
		nopaxos: nopaxos,
		port:    member.ProtocolPort,
		mu:      sync.Mutex{},
	}
	return server
}

// Server implements the NOPaxos consensus protocol server
type Server struct {
	nopaxos *nopaxos.NOPaxos
	server  *grpc.Server
	port    int
	mu      sync.Mutex
	started bool
}

// Start starts the NOPaxos server
func (s *Server) Start() error {
	logger.Warningf("[Debug by lz] Start NOPaxos server")
	s.mu.Lock()

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	if err != nil {
		return err
	}

	s.server = grpc.NewServer()
	nopaxos.RegisterClientServiceServer(s.server, s.nopaxos)
	nopaxos.RegisterReplicaServiceServer(s.server, s.nopaxos)
	s.mu.Unlock()
	go s.server.Serve(lis)
	return s.WaitForReady()
}

// WaitForReady waits for the NOPaxos server to finish startup
func (s *Server) WaitForReady() error {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	s.nopaxos.Watch(func(status nopaxos.Status) {
		s.mu.Lock()
		if !s.started && status == nopaxos.StatusNormal {
			wg.Done()
			s.started = true
		}
		s.mu.Unlock()
	})
	wg.Wait()
	return nil
}

// timeout times out the server to start a view change
func (s *Server) timeout() {
	s.nopaxos.Timeout()
}

// Stop shuts down the NOPaxos server
func (s *Server) Stop() error {
	logger.Warningf("[Debug by lz] Stop NOPaxos server")
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.server != nil {
		s.server.Stop()
	}
	return nil
}

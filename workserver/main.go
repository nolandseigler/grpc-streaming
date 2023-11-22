/*
 *
 * Copyright 2015 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

// Package main implements a server for Greeter service.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/google/uuid"
	pb "github.com/nolandseigler/grpc_streaming/protos/work"
	"google.golang.org/grpc"
)

var (
	port    = flag.Int("port", 50051, "The server port")
	randers = rand.New(rand.NewSource(time.Now().Unix()))
)

type subKeeper struct {
	m           sync.Mutex
	subs        []*subscriber
	subKeyToIdx map[string]int
	hashPrefix  string
	randers     *rand.Rand
}

func (s *subKeeper) getSubKey(subID string) string {
	return fmt.Sprintf("%s:%s", s.hashPrefix, subID)
}

func (s *subKeeper) getSub(subID string) (*subscriber, bool) {
	idx, ok := s.subKeyToIdx[s.getSubKey(subID)]
	if !ok {
		return nil, ok
	}

	return s.subs[idx], true
}

func (s *subKeeper) addSub(sub *subscriber) {
	key := s.getSubKey(sub.ID)
	s.subs = append(s.subs, sub)
	s.subKeyToIdx[key] = len(s.subs) - 1
}

func (s *subKeeper) getRandSubIdx() (int, error) {
	if len(s.subs) == 0 {
		return 0, fmt.Errorf("no subs in keeper")
	}

	return s.randers.Intn(len(s.subs)), nil
}

func (s *subKeeper) AddSub(sub *subscriber) error {
	s.m.Lock()
	defer s.m.Unlock()
	if _, ok := s.getSub(sub.ID); ok {
		return fmt.Errorf("sub already exists")
	}

	s.addSub(sub)

	return nil
}

func (s *subKeeper) GetRandSub(sub *subscriber) (*subscriber, error) {
	s.m.Lock()
	defer s.m.Unlock()
	
	idx, errs.getRandSubIdx()

	return nil, nil
}

// server is used to implement helloworld.GreeterServer.
type server struct {
	pb.UnimplementedWorkServer
}

// StartWork implements helloworld.GreeterServer
func (s *server) StartWork(ctx context.Context, in *pb.StartWorkRequest) (*pb.StartWorkResponse, error) {
	log.Printf("Received: %s, %v", in.Name, in.Task)

	// fire off a go routine that simulates work and will publish status back on some selected stream or fail
	go func() {

	}()

	return &pb.StartWorkResponse{WorkId: uuid.New().String()}, nil
}

type subscriber struct {
	ID     string
	Stream pb.Work_SubscribeToRoundRobinWorkStatusServer
}

// ListFeatures lists all features contained within the given bounding Rectangle.
func (s *server) SubscribeToRoundRobinWorkStatus(in *pb.SubscribeRequest, stream pb.Work_SubscribeToRoundRobinWorkStatusServer) error {

	if _, ok := subscribers.Load(subKey); ok {
		return fmt.Errorf("subscriber id already subscribed")
	}

	subscribers.Store(subKey, &subscriber{ID: in.SubscriberId, Stream: stream})

	ctx := stream.Context()
	// Keep this scope alive because once this scope exits - the stream is closed
	for {
		select {
		// later use this to disconnect on client request.
		// by sending on channel when unsubscribe hit for sub id.
		// case <-fin:
		// 	log.Printf("Closing stream for SubscriberId: %s", in.SubscriberId)
		// 	return nil
		case <-ctx.Done():
			log.Printf("SubscriberId %s has disconnected", in.SubscriberId)
			return nil
		}
	}

	// return nil
}

func main() {
	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterWorkServer(s, &server{})
	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

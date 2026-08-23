package grpc

import (
	"context"
	"log"
	"net"
	"sync"

	"matching-engine/engine"
	"matching-engine/kafka"
	"matching-engine/models"
	pb "matching-engine/proto"

	"google.golang.org/grpc"
)

type MatchingEngineServer struct {
	pb.UnimplementedMatchingEngineServer
	orderBooks   map[string]*engine.OrderBook
	obMu         sync.RWMutex
	kafkaProducer *kafka.TradeProducer
}

func NewMatchingEngineServer(kafkaProd *kafka.TradeProducer) *MatchingEngineServer {
	return &MatchingEngineServer{
		orderBooks:   make(map[string]*engine.OrderBook),
		kafkaProducer: kafkaProd,
	}
}

func (s *MatchingEngineServer) getOrderBook(symbol string) *engine.OrderBook {
	s.obMu.RLock()
	ob, exists := s.orderBooks[symbol]
	s.obMu.RUnlock()
	
	if !exists {
		s.obMu.Lock()
		ob, exists = s.orderBooks[symbol]
		if !exists {
			ob = engine.NewOrderBook(symbol)
			s.orderBooks[symbol] = ob
		}
		s.obMu.Unlock()
	}
	return ob
}

func (s *MatchingEngineServer) MatchOrder(ctx context.Context, req *pb.OrderMessage) (*pb.MatchResponse, error) {
	order := models.Order{
		OrderID:   req.GetOrderId(),
		UserID:    req.GetUserId(),
		Symbol:    req.GetSymbol(),
		Type:      req.GetType(),
		Side:      req.GetSide(),
		Price:     req.GetPrice(),
		Amount:    req.GetAmount(),
		Timestamp: req.GetTimestamp(),
	}

	ob := s.getOrderBook(req.GetSymbol())
	trades := ob.ProcessOrder(&order)
	for _, trade := range trades {
		s.kafkaProducer.PublishTrade(trade)
	}

	return &pb.MatchResponse{
		Success: true,
		Message: "Order processed successfully",
	}, nil
}

func (s *MatchingEngineServer) CancelOrder(ctx context.Context, req *pb.CancelMessage) (*pb.CancelResponse, error) {
	s.obMu.RLock()
	var found bool
	for _, ob := range s.orderBooks {
		if ob.CancelOrder(req.GetOrderId()) {
			found = true
			break
		}
	}
	s.obMu.RUnlock()

	if found {
		s.kafkaProducer.PublishCancelConfirmation(req.GetOrderId())
	}

	return &pb.CancelResponse{
		Success: true,
		Message: "Order cancellation processed",
	}, nil
}

func StartGrpcServer(port string, kp *kafka.TradeProducer) {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen on port %s: %v", port, err)
	}

	s := grpc.NewServer()
	pb.RegisterMatchingEngineServer(s, NewMatchingEngineServer(kp))

	log.Printf("gRPC server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

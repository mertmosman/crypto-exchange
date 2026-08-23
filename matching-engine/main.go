package main

import (
	"log"
	"matching-engine/kafka"
    matching_grpc "matching-engine/grpc"
)

func main() {
	log.Println("Kripto Para Borsası - Eşleştirme Motoru Başlıyor...")

	// Kafka bağlantı ayarları (Sadece producer olarak, trade'leri basmak için)
	brokers := []string{"localhost:9092"}
	tradeTopic := "trades"
    cancelTopic := "canceled_orders"

    producer := kafka.NewTradeProducer(brokers, tradeTopic, cancelTopic)

	// gRPC Sunucusunu başlat (Port 50051)
    // Artık Kafka'dan emir okumuyoruz, emirleri Java'dan gRPC ile anında alıyoruz.
    matching_grpc.StartGrpcServer(":50051", producer)
}

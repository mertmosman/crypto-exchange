package main

import (
	"log"
	"matching-engine/engine"
	"matching-engine/kafka"
)

func main() {
	log.Println("Kripto Para Borsası - Eşleştirme Motoru Başlıyor...")

	// BTC_USDT çifti için in-memory OrderBook oluşturuluyor
	ob := engine.NewOrderBook("BTC_USDT")

	// Kafka bağlantı ayarları
	brokers := []string{"localhost:9092"}
	orderTopic := "orders"
	tradeTopic := "trades"

	// Kafka'yı başlat ve gelen emirleri Eşleştirme Motoruna besle
	// Bu fonksiyon sonsuz döngüde (blocking) çalışacaktır.
	kafka.StartKafkaConsumerAndProducer(brokers, orderTopic, tradeTopic, ob)
}

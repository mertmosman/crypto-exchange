package kafka

import (
	"context"
	"encoding/json"
	"log"
	"matching-engine/engine"
	"matching-engine/models"

	kafka "github.com/segmentio/kafka-go"
)

func StartKafkaConsumerAndProducer(brokers []string, orderTopic string, tradeTopic string, ob *engine.OrderBook) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		Topic:   orderTopic,
		GroupID: "matching-engine-group",
	})

	writer := &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Topic:    tradeTopic,
		Balancer: &kafka.LeastBytes{},
	}

	log.Println("Go Eşleştirme Motoru Kafka'yı dinlemeye basladi...")

	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Kafka okuma hatası: %v", err)
			continue
		}

		var order models.Order
		err = json.Unmarshal(m.Value, &order)
		if err != nil {
			log.Printf("JSON parse hatası: %v", err)
			continue
		}

		if order.Type == "CANCEL" {
			// İptal işlemini OrderBook'ta dene
			success := ob.CancelOrder(order.OrderID, order.Side)
			if success {
				// İptal başarılıysa, bunu Java'ya bildirmemiz gerek
				// Bunu ayrı bir topic (canceled_orders) üzerinden yapalım ki trade ile karışmasın
				// Ancak performans için direkt Writer aç kapa yerine global bir writer kullanabiliriz
				// Şimdilik pratik olarak yeni bir writer kullanalım (veya yukarıda tanımlayabiliriz)
				cancelWriter := &kafka.Writer{
					Addr:     kafka.TCP(brokers...),
					Topic:    "canceled_orders",
					Balancer: &kafka.LeastBytes{},
				}
				cancelBytes, _ := json.Marshal(order)
				err = cancelWriter.WriteMessages(context.Background(),
					kafka.Message{
						Key:   []byte(order.Symbol),
						Value: cancelBytes,
					},
				)
				if err != nil {
					log.Printf("İptal onayı Kafka'ya yazılamadı: %v", err)
				} else {
					log.Printf("Emir iptali başarılı ve onay gönderildi: %s", order.OrderID)
				}
				cancelWriter.Close()
			} else {
				log.Printf("İptal edilecek emir bulunamadı (zaten eşleşmiş olabilir): %s", order.OrderID)
			}
			continue
		}

		// Normal emir ise eşleştirme motoruna ver
		trades := ob.ProcessOrder(&order)

		// Eğer eşleşme(ler) olduysa Kafka'ya trade (işlem) olarak fırlat
		for _, trade := range trades {
			tradeBytes, _ := json.Marshal(trade)
			err = writer.WriteMessages(context.Background(),
				kafka.Message{
					Key:   []byte(trade.Symbol),
					Value: tradeBytes,
				},
			)
			if err != nil {
				log.Printf("Trade Kafka'ya yazilamadi: %v", err)
			} else {
				log.Printf("Eşleşme Kafka'ya gönderildi! TradeID: %s, Fiyat: %d, Miktar: %d", trade.TradeID, trade.Price, trade.Amount)
			}
		}
	}
}

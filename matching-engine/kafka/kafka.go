package kafka

import (
	"context"
	"encoding/json"
	"log"
	"matching-engine/models"

	kafka "github.com/segmentio/kafka-go"
)

type TradeProducer struct {
	tradeWriter  *kafka.Writer
	cancelWriter *kafka.Writer
}

func NewTradeProducer(brokers []string, tradeTopic string, cancelTopic string) *TradeProducer {
	return &TradeProducer{
		tradeWriter: &kafka.Writer{
			Addr:     kafka.TCP(brokers...),
			Topic:    tradeTopic,
			Balancer: &kafka.LeastBytes{},
		},
		cancelWriter: &kafka.Writer{
			Addr:     kafka.TCP(brokers...),
			Topic:    cancelTopic,
			Balancer: &kafka.LeastBytes{},
		},
	}
}

func (tp *TradeProducer) PublishTrade(trade models.Trade) {
	tradeBytes, _ := json.Marshal(trade)
	err := tp.tradeWriter.WriteMessages(context.Background(),
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

func (tp *TradeProducer) PublishCancelConfirmation(orderId string) {
	order := models.Order{OrderID: orderId}
	cancelBytes, _ := json.Marshal(order)
	err := tp.cancelWriter.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte("CANCEL"),
			Value: cancelBytes,
		},
	)
	if err != nil {
		log.Printf("İptal onayı Kafka'ya yazılamadı: %v", err)
	} else {
		log.Printf("Emir iptali başarılı ve onay gönderildi: %s", orderId)
	}
}


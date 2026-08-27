package engine

import (
	"matching-engine/models"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Price-Time Priority Eşleştirme Motoru
type OrderBook struct {
	Symbol string
	Bids   []*models.Order // Alış emirleri (En yüksek fiyat en üstte)
	Asks   []*models.Order // Satış emirleri (En düşük fiyat en üstte)
	mu     sync.Mutex
}

func NewOrderBook(symbol string) *OrderBook {
	return &OrderBook{
		Symbol: symbol,
		Bids:   make([]*models.Order, 0),
		Asks:   make([]*models.Order, 0),
	}
}

func (ob *OrderBook) ProcessOrder(order *models.Order) []models.Trade {
	ob.mu.Lock()
	defer ob.mu.Unlock()

	var trades []models.Trade

	if order.Side == "BUY" {
		trades = ob.matchBuy(order)
	} else if order.Side == "SELL" {
		trades = ob.matchSell(order)
	}
	return trades
}

func (ob *OrderBook) matchBuy(buyOrder *models.Order) []models.Trade {
	var trades []models.Trade

	// Satış emirlerini dolaş (En ucuzdan başlayarak)
	for len(ob.Asks) > 0 && buyOrder.Amount > 0 {
		bestAsk := ob.Asks[0]

		// Alıcının teklifi, satıcının istediğinden düşükse eşleşme biter.
		if buyOrder.Price < bestAsk.Price {
			break
		}

		// Eşleşme bulundu! Miktarı belirle (Hangisi küçükse)
		tradeAmount := buyOrder.Amount
		if bestAsk.Amount < tradeAmount {
			tradeAmount = bestAsk.Amount
		}

		// Trade oluştur (Fiyat her zaman tahtadaki/pasif emrin fiyatı olur)
		trade := models.Trade{
			TradeID:       uuid.New().String(),
			BuyerOrderID:  buyOrder.OrderID,
			SellerOrderID: bestAsk.OrderID,
			BuyerID:       buyOrder.UserID,
			SellerID:      bestAsk.UserID,
			Symbol:        ob.Symbol,
			Price:         bestAsk.Price,
			Amount:        tradeAmount,
			Timestamp:     time.Now().UnixMilli(),
		}
		trades = append(trades, trade)

		// Miktarları düş
		buyOrder.Amount -= tradeAmount
		bestAsk.Amount -= tradeAmount

		// Satıcının emri bittiyse tahtadan sil
		if bestAsk.Amount == 0 {
			ob.Asks = ob.Asks[1:]
		}
	}

	// Alıcının hala emri kaldıysa, Bids (Alışlar) tahtasına ekle
	if buyOrder.Amount > 0 {
		ob.Bids = append(ob.Bids, buyOrder)
		// Fiyata göre Büyükten Küçüğe, Zamana göre Eskiden Yeniye sırala (Price-Time)
		sort.Slice(ob.Bids, func(i, j int) bool {
			if ob.Bids[i].Price == ob.Bids[j].Price {
				return ob.Bids[i].Timestamp < ob.Bids[j].Timestamp
			}
			return ob.Bids[i].Price > ob.Bids[j].Price
		})
	}

	return trades
}

func (ob *OrderBook) matchSell(sellOrder *models.Order) []models.Trade {
	var trades []models.Trade

	// Alış emirlerini dolaş (En pahalıdan başlayarak)
	for len(ob.Bids) > 0 && sellOrder.Amount > 0 {
		bestBid := ob.Bids[0]

		// Satıcının teklifi, alıcının verdiğinden yüksekse eşleşme biter.
		if sellOrder.Price > bestBid.Price {
			break
		}

		tradeAmount := sellOrder.Amount
		if bestBid.Amount < tradeAmount {
			tradeAmount = bestBid.Amount
		}

		trade := models.Trade{
			TradeID:       uuid.New().String(),
			BuyerOrderID:  bestBid.OrderID,
			SellerOrderID: sellOrder.OrderID,
			BuyerID:       bestBid.UserID,
			SellerID:      sellOrder.UserID,
			Symbol:        ob.Symbol,
			Price:         bestBid.Price,
			Amount:        tradeAmount,
			Timestamp:     time.Now().UnixMilli(),
		}
		trades = append(trades, trade)

		sellOrder.Amount -= tradeAmount
		bestBid.Amount -= tradeAmount

		if bestBid.Amount == 0 {
			ob.Bids = ob.Bids[1:]
		}
	}

	if sellOrder.Amount > 0 {
		ob.Asks = append(ob.Asks, sellOrder)
		// Fiyata göre Küçükten Büyüğe, Zamana göre Eskiden Yeniye sırala (Price-Time)
		sort.Slice(ob.Asks, func(i, j int) bool {
			if ob.Asks[i].Price == ob.Asks[j].Price {
				return ob.Asks[i].Timestamp < ob.Asks[j].Timestamp
			}
			return ob.Asks[i].Price < ob.Asks[j].Price
		})
	}

	return trades
}

func (ob *OrderBook) CancelOrder(orderId string) bool {
	ob.mu.Lock()
	defer ob.mu.Unlock()

	// Önce Bids (Alış) tahtasında ara
	for i, order := range ob.Bids {
		if order.OrderID == orderId {
			// Slice'tan sil
			ob.Bids = append(ob.Bids[:i], ob.Bids[i+1:]...)
			return true
		}
	}
	
	// Sonra Asks (Satış) tahtasında ara
	for i, order := range ob.Asks {
		if order.OrderID == orderId {
			// Slice'tan sil
			ob.Asks = append(ob.Asks[:i], ob.Asks[i+1:]...)
			return true
		}
	}
	
	// Eğer bulunamadıysa zaten eşleşmiş veya hiç gelmemiştir
	return false
}

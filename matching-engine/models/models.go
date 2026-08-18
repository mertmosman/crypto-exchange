package models

type Order struct {
	OrderID   string `json:"orderId"`
	UserID    string `json:"userId"`
	Symbol    string `json:"symbol"`
	Side      string `json:"side"`      // "BUY" or "SELL"
	Type      string `json:"type"`      // "LIMIT"
	Price     int64  `json:"price"`     // Satoshi
	Amount    int64  `json:"amount"`    // Satoshi
	Status    string `json:"status"`    // "PENDING" vb.
	Timestamp int64  `json:"timestamp"` // Milisaniye
}

type Trade struct {
	TradeID       string `json:"tradeId"`
	BuyerOrderID  string `json:"buyerOrderId"`
	SellerOrderID string `json:"sellerOrderId"`
	BuyerID       string `json:"buyerId"`
	SellerID      string `json:"sellerId"`
	Symbol        string `json:"symbol"`
	Price         int64  `json:"price"`  // Eşleşme Fiyatı
	Amount        int64  `json:"amount"` // Eşleşen Miktar
	Timestamp     int64  `json:"timestamp"`
}

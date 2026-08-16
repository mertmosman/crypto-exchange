package com.crypto.exchange_api.model;

import lombok.Data;

@Data
public class Trade {
    private String tradeId;
    
    private String buyerOrderId;
    private String sellerOrderId;
    
    private String buyerId;
    private String sellerId;
    
    private String symbol; // Örn: BTC_USDT
    
    private Long price; // Eşleşen nihai fiyat (Satoshi)
    private Long amount; // Eşleşen miktar (Satoshi)
    
    private Long timestamp;
}

package com.crypto.exchange_api.model;

import jakarta.persistence.*;
import lombok.Data;

@Data
@Entity
@Table(name = "orders")
public class Order {
    
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private String orderId;
    
    private String userId;
    private String symbol; // Örn: BTC_USDT
    
    private String side; // BUY veya SELL
    private String type; // LIMIT veya MARKET
    
    private Long price; // Satoshi formatında
    private Long amount; // Satoshi formatında
    
    private String status; // PENDING (Bekliyor), FILLED (Gerçekleşti), CANCELED
    
    private Long timestamp;
}

package com.crypto.exchange_api.model;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;

@Data
@Entity
@Table(name = "wallets")
public class Wallet {
    
    @Id
    private String userId;
    
    // Kullanıcının kullanılabilir bakiyeleri (Satoshi mantığı: 10^8 formatında)
    private Long usdtBalance = 0L;
    private Long btcBalance = 0L;
    
    // Açık emirlerde (LIMIT orders) bloke edilmiş bakiyeler
    // Pre-Block (Ön-Bloke) mantığı burada işler.
    private Long lockedUsdtBalance = 0L;
    private Long lockedBtcBalance = 0L;
}

package com.crypto.exchange_api.controller;

import com.crypto.exchange_api.model.Wallet;
import com.crypto.exchange_api.repository.WalletRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/wallets")
@RequiredArgsConstructor
public class WalletController {

    private final WalletRepository walletRepository;

    // Test amaçlı sahte bakiye yükleme uç noktası
    @PostMapping("/seed")
    public ResponseEntity<Wallet> seedWallet(@RequestParam String userId, @RequestParam Long usdtAmount, @RequestParam Long btcAmount) {
        Wallet wallet = walletRepository.findById(userId).orElse(new Wallet());
        wallet.setUserId(userId);
        
        // Örn: usdtAmount = 10000_00000000 (Satoshi formatında 10.000 Dolar)
        wallet.setUsdtBalance(wallet.getUsdtBalance() + usdtAmount);
        wallet.setBtcBalance(wallet.getBtcBalance() + btcAmount);
        
        return ResponseEntity.ok(walletRepository.save(wallet));
    }
    
    @GetMapping("/{userId}")
    public ResponseEntity<Wallet> getWallet(@PathVariable String userId) {
        return walletRepository.findById(userId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }
}

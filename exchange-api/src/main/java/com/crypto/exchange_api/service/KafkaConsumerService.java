package com.crypto.exchange_api.service;

import com.crypto.exchange_api.model.Order;
import com.crypto.exchange_api.model.Trade;
import com.crypto.exchange_api.model.Wallet;
import com.crypto.exchange_api.repository.OrderRepository;
import com.crypto.exchange_api.repository.WalletRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaConsumerService {

    private final WalletRepository walletRepository;
    private final OrderRepository orderRepository;

    private static final long SATOSHI_SCALE = 100_000_000L;

    private final com.fasterxml.jackson.databind.ObjectMapper objectMapper = new com.fasterxml.jackson.databind.ObjectMapper();

    @KafkaListener(topics = "trades", groupId = "exchange-api-group")
    @Transactional
    public void consumeTrade(String message) {
        try {
            Trade trade = objectMapper.readValue(message, Trade.class);
            log.info("==== YENI ESLESME (TRADE) ALINDI ====");
            log.info("Trade ID: {} | Fiyat: {} | Miktar: {}", trade.getTradeId(), trade.getPrice(), trade.getAmount());

            // 1. ALICI (BUYER) KESİNLEŞTİRMESİ (Settlement)
            Wallet buyerWallet = walletRepository.findById(trade.getBuyerId()).orElse(null);
            Order buyerOrder = orderRepository.findById(trade.getBuyerOrderId()).orElse(null);

            if (buyerWallet != null && buyerOrder != null) {
                long actualCostUsdt = (trade.getPrice() * trade.getAmount()) / SATOSHI_SCALE;
                
                long initialLockedUsdtForThisTradePart = (buyerOrder.getPrice() * trade.getAmount()) / SATOSHI_SCALE;
                long refundUsdt = initialLockedUsdtForThisTradePart - actualCostUsdt;

                buyerWallet.setLockedUsdtBalance(buyerWallet.getLockedUsdtBalance() - initialLockedUsdtForThisTradePart);
                buyerWallet.setBtcBalance(buyerWallet.getBtcBalance() + trade.getAmount());
                
                if (refundUsdt > 0) {
                    buyerWallet.setUsdtBalance(buyerWallet.getUsdtBalance() + refundUsdt);
                }
                walletRepository.save(buyerWallet);
            }

            // 2. SATICI (SELLER) KESİNLEŞTİRMESİ (Settlement)
            Wallet sellerWallet = walletRepository.findById(trade.getSellerId()).orElse(null);
            Order sellerOrder = orderRepository.findById(trade.getSellerOrderId()).orElse(null);

            if (sellerWallet != null && sellerOrder != null) {
                sellerWallet.setLockedBtcBalance(sellerWallet.getLockedBtcBalance() - trade.getAmount());
                long earnedUsdt = (trade.getPrice() * trade.getAmount()) / SATOSHI_SCALE;
                sellerWallet.setUsdtBalance(sellerWallet.getUsdtBalance() + earnedUsdt);
                walletRepository.save(sellerWallet);
            }
            log.info("==== TRADE SETTLEMENT BASARILI ====");
        } catch (Exception e) {
            log.error("Trade okuma hatası: {}", e.getMessage());
        }
    }

    @KafkaListener(topics = "canceled_orders", groupId = "exchange-api-group")
    @Transactional
    public void consumeCanceledOrder(String message) {
        try {
            Order canceledOrderEvent = objectMapper.readValue(message, Order.class);
            log.info("==== EMIR IPTAL ONAYI ALINDI: {} ====", canceledOrderEvent.getOrderId());

            Order order = orderRepository.findById(canceledOrderEvent.getOrderId()).orElse(null);
            if (order != null && "CANCEL_REQUESTED".equals(order.getStatus())) {
                Wallet wallet = walletRepository.findById(order.getUserId()).orElse(null);
                if (wallet != null) {
                    if ("BUY".equalsIgnoreCase(order.getSide())) {
                        long lockedUsdt = (order.getPrice() * order.getAmount()) / SATOSHI_SCALE;
                        wallet.setLockedUsdtBalance(wallet.getLockedUsdtBalance() - lockedUsdt);
                        wallet.setUsdtBalance(wallet.getUsdtBalance() + lockedUsdt);
                    } else if ("SELL".equalsIgnoreCase(order.getSide())) {
                        long lockedBtc = order.getAmount();
                        wallet.setLockedBtcBalance(wallet.getLockedBtcBalance() - lockedBtc);
                        wallet.setBtcBalance(wallet.getBtcBalance() + lockedBtc);
                    }
                    walletRepository.save(wallet);
                }
                order.setStatus("CANCELLED");
                orderRepository.save(order);
                log.info("Emir ({}) iptal edildi ve bakiye iade edildi.", order.getOrderId());
            }
        } catch (Exception e) {
            log.error("Canceled Order okuma hatası: {}", e.getMessage());
        }
    }
}

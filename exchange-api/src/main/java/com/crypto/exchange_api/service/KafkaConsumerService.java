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
    private final org.springframework.amqp.rabbit.core.RabbitTemplate rabbitTemplate;

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
            Wallet buyerWallet = walletRepository.findByIdForUpdate(trade.getBuyerId()).orElse(null);
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
                
                // Siparişin miktarını düş ve durumunu güncelle
                buyerOrder.setAmount(buyerOrder.getAmount() - trade.getAmount());
                buyerOrder.setStatus(buyerOrder.getAmount() <= 0 ? "FILLED" : "PARTIAL_FILLED");
                orderRepository.save(buyerOrder);
            }

            // 2. SATICI (SELLER) KESİNLEŞTİRMESİ (Settlement)
            Wallet sellerWallet = walletRepository.findByIdForUpdate(trade.getSellerId()).orElse(null);
            Order sellerOrder = orderRepository.findById(trade.getSellerOrderId()).orElse(null);

            if (sellerWallet != null && sellerOrder != null) {
                sellerWallet.setLockedBtcBalance(sellerWallet.getLockedBtcBalance() - trade.getAmount());
                long earnedUsdt = (trade.getPrice() * trade.getAmount()) / SATOSHI_SCALE;
                sellerWallet.setUsdtBalance(sellerWallet.getUsdtBalance() + earnedUsdt);
                walletRepository.save(sellerWallet);

                // Siparişin miktarını düş ve durumunu güncelle
                sellerOrder.setAmount(sellerOrder.getAmount() - trade.getAmount());
                sellerOrder.setStatus(sellerOrder.getAmount() <= 0 ? "FILLED" : "PARTIAL_FILLED");
                orderRepository.save(sellerOrder);
            }
            log.info("==== TRADE SETTLEMENT BASARILI ====");

            // RabbitMQ'ya asenkron e-posta bildirimleri gönder (Email Service için)
            rabbitTemplate.convertAndSend("email_queue", "Trade_Success: Buyer " + trade.getBuyerId() + " bought " + trade.getAmount() + " BTC");
            rabbitTemplate.convertAndSend("email_queue", "Trade_Success: Seller " + trade.getSellerId() + " sold " + trade.getAmount() + " BTC");

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
                Wallet wallet = walletRepository.findByIdForUpdate(order.getUserId()).orElse(null);
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

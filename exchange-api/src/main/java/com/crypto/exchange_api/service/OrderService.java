package com.crypto.exchange_api.service;

import com.crypto.exchange_api.model.Order;
import com.crypto.exchange_api.model.Wallet;
import com.crypto.exchange_api.repository.OrderRepository;
import com.crypto.exchange_api.repository.WalletRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class OrderService {

    private final WalletRepository walletRepository;
    private final OrderRepository orderRepository;
    private final KafkaProducerService kafkaProducerService;

    // Satoshi (10^8) çarpanı sabitimiz
    private static final long SATOSHI_SCALE = 100_000_000L;

    @Transactional
    public Order placeOrder(Order order) {
        // 1. Kullanıcının cüzdanını bul
        Wallet wallet = walletRepository.findById(order.getUserId())
                .orElseThrow(() -> new RuntimeException("Cüzdan bulunamadı: " + order.getUserId()));

        // 2. PRE-BLOCK (Ön-Bloke) Mantığı
        if ("BUY".equalsIgnoreCase(order.getSide())) {
            // Alış yapıyorsa USDT gereklidir.
            // Küsürat hatası yapmamak için Satoshi matematiği uygulanır.
            long requiredUsdt = (order.getPrice() * order.getAmount()) / SATOSHI_SCALE;

            if (wallet.getUsdtBalance() < requiredUsdt) {
                throw new RuntimeException("Yetersiz USDT bakiyesi!");
            }
            
            // Bakiyeyi kullanılabilir tutardan düş, bloke (locked) tutara ekle
            wallet.setUsdtBalance(wallet.getUsdtBalance() - requiredUsdt);
            wallet.setLockedUsdtBalance(wallet.getLockedUsdtBalance() + requiredUsdt);

        } else if ("SELL".equalsIgnoreCase(order.getSide())) {
            // Satış yapıyorsa doğrudan SATILACAK COIN (Örn BTC) miktarı kontrol edilir
            long requiredBtc = order.getAmount();
            
            if (wallet.getBtcBalance() < requiredBtc) {
                throw new RuntimeException("Yetersiz BTC bakiyesi!");
            }
            
            // Bakiyeyi düş, bloke tutara ekle
            wallet.setBtcBalance(wallet.getBtcBalance() - requiredBtc);
            wallet.setLockedBtcBalance(wallet.getLockedBtcBalance() + requiredBtc);
        } else {
            throw new IllegalArgumentException("Bilinmeyen işlem yönü (Side): Sadece BUY veya SELL olabilir.");
        }

        // 3. Veritabanını güncelle (Hibernate bunu Transaction bitiminde yapar ama biz yine de save çağırıyoruz)
        walletRepository.save(wallet);

        // 4. Emri PENDING (Bekliyor) olarak veritabanına kaydet
        order.setStatus("PENDING");
        order.setTimestamp(System.currentTimeMillis());
        Order savedOrder = orderRepository.save(order);

        // 5. Emri Eşleştirme Motoru (Go) için Kafka'ya fırlat!
        // Eğer Spring Transaction hata almazsa (Rollback olmazsa) Kafka'ya emir gider.
        kafkaProducerService.sendOrderToMatchingEngine(savedOrder);

        return savedOrder;
    }

    @Transactional
    public Order cancelOrder(String orderId) {
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new RuntimeException("Emir bulunamadı: " + orderId));

        if (!"PENDING".equals(order.getStatus())) {
            throw new RuntimeException("Sadece PENDING durumundaki emirler iptal edilebilir.");
        }

        // Durumu güncelle
        order.setStatus("CANCEL_REQUESTED");
        Order savedOrder = orderRepository.save(order);

        // Kafka'ya iptal isteği olarak gönder (Type'ı CANCEL yaparak)
        // Orijinal emrin kopyasını alıyoruz ki veritabanındaki "LIMIT" tipi değişmesin
        Order cancelMsg = new Order();
        cancelMsg.setOrderId(order.getOrderId());
        cancelMsg.setUserId(order.getUserId());
        cancelMsg.setSymbol(order.getSymbol());
        cancelMsg.setSide(order.getSide());
        cancelMsg.setPrice(order.getPrice());
        cancelMsg.setAmount(order.getAmount());
        cancelMsg.setType("CANCEL"); // Go motoru bu flag ile iptal olduğunu anlayacak
        cancelMsg.setTimestamp(System.currentTimeMillis());

        kafkaProducerService.sendOrderToMatchingEngine(cancelMsg);

        return savedOrder;
    }

    public java.util.List<Order> getUserOrders(String userId) {
        return orderRepository.findByUserId(userId);
    }
}

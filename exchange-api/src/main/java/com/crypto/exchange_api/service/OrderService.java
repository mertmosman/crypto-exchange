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

    // gRPC client tanımı
    @net.devh.boot.grpc.client.inject.GrpcClient("matching-engine")
    private com.crypto.exchange_api.grpc.MatchingEngineGrpc.MatchingEngineBlockingStub matchingEngineStub;

    @Transactional
    public Order placeOrder(Order order) {
        // 1. Kullanıcının cüzdanını bul ve KİLİTLE (Pessimistic Write Lock)
        Wallet wallet = walletRepository.findByIdForUpdate(order.getUserId())
                .orElseThrow(() -> new RuntimeException("Cüzdan bulunamadı: " + order.getUserId()));

        // 2. PRE-BLOCK (Ön-Bloke) Mantığı
        if ("BUY".equalsIgnoreCase(order.getSide())) {
            long requiredUsdt = (order.getPrice() * order.getAmount()) / SATOSHI_SCALE;

            if (wallet.getUsdtBalance() < requiredUsdt) {
                throw new RuntimeException("Yetersiz USDT bakiyesi!");
            }
            
            wallet.setUsdtBalance(wallet.getUsdtBalance() - requiredUsdt);
            wallet.setLockedUsdtBalance(wallet.getLockedUsdtBalance() + requiredUsdt);

        } else if ("SELL".equalsIgnoreCase(order.getSide())) {
            long requiredBtc = order.getAmount();
            
            if (wallet.getBtcBalance() < requiredBtc) {
                throw new RuntimeException("Yetersiz BTC bakiyesi!");
            }
            
            wallet.setBtcBalance(wallet.getBtcBalance() - requiredBtc);
            wallet.setLockedBtcBalance(wallet.getLockedBtcBalance() + requiredBtc);
        } else {
            throw new IllegalArgumentException("Bilinmeyen işlem yönü (Side): Sadece BUY veya SELL olabilir.");
        }

        // 3. Veritabanını güncelle
        walletRepository.save(wallet);

        // 4. Emri PENDING (Bekliyor) olarak veritabanına kaydet
        order.setStatus("PENDING");
        order.setTimestamp(System.currentTimeMillis());
        Order savedOrder = orderRepository.save(order);

        // 5. Emri yedekleme/Log için Kafka'ya fırlat
        kafkaProducerService.sendOrderToMatchingEngine(savedOrder);

        // 6. Işık hızı (gRPC) ile eşleştirme motorunu çağır!
        try {
            com.crypto.exchange_api.grpc.OrderMessage grpcOrder = com.crypto.exchange_api.grpc.OrderMessage.newBuilder()
                    .setOrderId(savedOrder.getOrderId())
                    .setUserId(savedOrder.getUserId())
                    .setSymbol(savedOrder.getSymbol())
                    .setType(savedOrder.getType())
                    .setSide(savedOrder.getSide())
                    .setPrice(savedOrder.getPrice())
                    .setAmount(savedOrder.getAmount())
                    .setTimestamp(savedOrder.getTimestamp())
                    .build();

            com.crypto.exchange_api.grpc.MatchResponse response = matchingEngineStub.matchOrder(grpcOrder);
            // System.out.println("gRPC Response: " + response.getMessage());
        } catch (Exception e) {
            System.err.println("gRPC Error: " + e.getMessage());
        }

        return savedOrder;
    }

    @Transactional
    public Order cancelOrder(String orderId) {
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new RuntimeException("Emir bulunamadı: " + orderId));

        if (!"PENDING".equals(order.getStatus())) {
            throw new RuntimeException("Sadece PENDING durumundaki emirler iptal edilebilir.");
        }

        order.setStatus("CANCEL_REQUESTED");
        Order savedOrder = orderRepository.save(order);

        Order cancelMsg = new Order();
        cancelMsg.setOrderId(order.getOrderId());
        cancelMsg.setUserId(order.getUserId());
        cancelMsg.setSymbol(order.getSymbol());
        cancelMsg.setSide(order.getSide());
        cancelMsg.setPrice(order.getPrice());
        cancelMsg.setAmount(order.getAmount());
        cancelMsg.setType("CANCEL");
        cancelMsg.setTimestamp(System.currentTimeMillis());

        // Yedekleme için Kafka'ya gönder
        kafkaProducerService.sendOrderToMatchingEngine(cancelMsg);

        // gRPC üzerinden anında iptal çağrısı yap
        try {
            com.crypto.exchange_api.grpc.CancelMessage grpcCancel = com.crypto.exchange_api.grpc.CancelMessage.newBuilder()
                    .setOrderId(order.getOrderId())
                    .build();

            com.crypto.exchange_api.grpc.CancelResponse response = matchingEngineStub.cancelOrder(grpcCancel);
        } catch (Exception e) {
            System.err.println("gRPC Cancel Error: " + e.getMessage());
        }

        return savedOrder;
    }

    public java.util.List<Order> getUserOrders(String userId) {
        return orderRepository.findByUserId(userId);
    }
}

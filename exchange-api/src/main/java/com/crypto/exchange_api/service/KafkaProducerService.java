package com.crypto.exchange_api.service;

import com.crypto.exchange_api.model.Order;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class KafkaProducerService {

    private final KafkaTemplate<String, Order> kafkaTemplate;

    public void sendOrderToMatchingEngine(Order order) {
        // "orders" topic'ine gönderiyoruz.
        // Anahtar (Key) olarak sembol (Örn: BTC_USDT) kullanıyoruz.
        // Bu sayede Kafka, aynı sembole ait emirleri aynı partition'a yazar ve eşleştirme sırası KESİNLİKLE bozulmaz.
        kafkaTemplate.send("orders", order.getSymbol(), order);
    }
}

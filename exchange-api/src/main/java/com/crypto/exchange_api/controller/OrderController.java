package com.crypto.exchange_api.controller;

import com.crypto.exchange_api.model.Order;
import com.crypto.exchange_api.service.OrderService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderService orderService;

    @PostMapping
    public ResponseEntity<Order> placeOrder(@RequestBody Order order) {
        // Pre-Block mantığını ve Kafka iletimini çalıştır
        Order savedOrder = orderService.placeOrder(order);
        return ResponseEntity.ok(savedOrder);
    }

    @org.springframework.web.bind.annotation.DeleteMapping("/{orderId}")
    public ResponseEntity<Order> cancelOrder(@org.springframework.web.bind.annotation.PathVariable String orderId) {
        Order cancelledOrder = orderService.cancelOrder(orderId);
        return ResponseEntity.ok(cancelledOrder);
    }

    @org.springframework.web.bind.annotation.GetMapping("/user/{userId}")
    public ResponseEntity<java.util.List<Order>> getUserOrders(@org.springframework.web.bind.annotation.PathVariable String userId) {
        return ResponseEntity.ok(orderService.getUserOrders(userId));
    }
}

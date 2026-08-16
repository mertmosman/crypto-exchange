package com.crypto.exchange_api.repository;

import com.crypto.exchange_api.model.Order;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface OrderRepository extends JpaRepository<Order, String> {
    java.util.List<Order> findByUserId(String userId);
}

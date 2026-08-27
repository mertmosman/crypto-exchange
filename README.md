# 🚀 High-Frequency Crypto Exchange Architecture

![Architecture](https://img.shields.io/badge/Architecture-Microservices-orange)
![Spring Boot](https://img.shields.io/badge/Java-Spring_Boot-green)
![Golang](https://img.shields.io/badge/Golang-Matching_Engine-blue)
![Python](https://img.shields.io/badge/Python-Notification_Worker-yellow)
![gRPC](https://img.shields.io/badge/gRPC-Microsecond_Latency-red)
![Kafka](https://img.shields.io/badge/Apache_Kafka-Event_Bus-black)
![RabbitMQ](https://img.shields.io/badge/RabbitMQ-Dead_Letter_Exchange-orange)
![Kubernetes](https://img.shields.io/badge/Kubernetes-Auto_Scaling-blue)

A production-ready, ultra-low latency cryptocurrency exchange matching engine and API. Built to handle **High-Frequency Trading (HFT)** scenarios with zero data loss, pessimistic locking, and dynamic auto-scaling.

## 📖 Project Overview
This project simulates the core infrastructure of a modern cryptocurrency exchange (like Binance or Coinbase). It is built to solve the most difficult challenges in fintech: processing thousands of financial transactions per second without losing data, keeping wallet balances 100% accurate under massive concurrency, and scaling automatically under heavy load.

## 🛠️ Tech Stack
- **API Gateway / Proxy:** Nginx (Rate Limiting, Reverse Proxy)
- **Backend API:** Java 17, Spring Boot 3, Spring Data JPA
- **Matching Engine:** Golang 1.21 (In-Memory, multi-symbol)
- **Asynchronous Workers:** Python 3 (Pika, Requests)
- **Inter-service Communication:** gRPC (Protocol Buffers)
- **Event Bus & Messaging:** Apache Kafka, RabbitMQ
- **Database:** PostgreSQL (HikariCP, Pessimistic Locking)
- **Containerization & Orchestration:** Docker, Docker Compose, Kubernetes (K8s)

## 🏗️ Architecture & Flow
This project is designed as a highly scalable microservices architecture where each component is chosen for its specific strengths:

1. **User Request:** A trader sends a Buy/Sell order to the Nginx Gateway. Nginx enforces rate limits (DDoS protection) and forwards it to the Java API.
2. **Pre-Block & Safety (Java):** The Java API checks the PostgreSQL database. Using **Pessimistic Write Locks**, it locks the user's wallet, deducts the required amount safely, and prevents "Lost Update" anomalies.
3. **Microsecond Dispatch (gRPC):** Instead of slow HTTP calls, Java sends the verified order to the Go Matching Engine via highly optimized gRPC.
4. **Matching Engine (Go):** The Go engine holds the entire OrderBook in RAM (Price-Time Priority). It dynamically spawns separate order books for multi-symbol trading (e.g., BTC_USDT, ETH_USDT).
5. **Event-Driven Settlement (Kafka):** Once a trade matches in Go, it instantly fires a `Trade` event to Kafka and continues processing new orders without waiting.
6. **Async Settlement (Java):** Java consumes Kafka trades at its own pace, unlocking balances and finalizing the transaction in the database.
7. **Notifications (RabbitMQ):** Java delegates email notifications to RabbitMQ. A Python worker consumes them. A **Dead Letter Exchange (DLX)** guarantees no email is lost even if the Python worker crashes.

## 📚 Service Layers (Microservices)

### 1. `exchange-api` (Java / Spring Boot)
The gateway and source of truth for user funds. Handles authentication (conceptually), REST endpoints, and wallet security. 
### 2. `matching-engine` (Golang)
The computational brain of the system. Has no database. Pure logic operating in memory for absolute lowest latency.
### 3. `notification-service` (Python)
A lightweight background worker dedicated entirely to I/O heavy tasks (like sending emails) so the core engine is never blocked.
### 4. `simulator` (Python Market Maker)
A multi-threaded HFT bot that bombards the system with thousands of random Buy/Sell orders to stress test rate limiting, locking, and match accuracy.

## 📡 API Endpoints (REST)

| Method | Endpoint | Description | Response |
| :--- | :--- | :--- | :--- |
| `POST` | `/api/orders/place` | Submit a new Buy/Sell order (Pre-blocks wallet balance) | `JSON (Order)` |
| `POST` | `/api/orders/cancel/{orderId}`| Cancel a pending order (Refunds locked balance) | `JSON (Order)` |
| `GET` | `/api/orders/user/{userId}` | Retrieve all orders and their current status | `JSON (Array)` |

**Example Request Payload (`/api/orders/place`):**
```json
{
  "userId": "user-123",
  "symbol": "BTC_USDT",
  "side": "BUY",
  "type": "LIMIT",
  "price": 6000000000000, 
  "amount": 500000000
}
```
> *Note: Price and amount are represented in Satoshis (10^8) to prevent floating-point calculation errors.*

## 🚀 How to Run (Locally)

### Prerequisites
- Docker & Docker Compose
- Maven (for Java)
- Go (for Matching Engine)
- Python (for Simulator/Notifications)

### Step 1: Start the Infrastructure
```bash
docker-compose up -d
```
*This starts PostgreSQL, Kafka, Zookeeper, RabbitMQ, and Redis.*

### Step 2: Start the Go Matching Engine
```bash
cd matching-engine
go run main.go
```
*Listens for gRPC calls on port `50051`.*

### Step 3: Start the Java API
```bash
cd exchange-api
mvn spring-boot:run
```
*Listens on port `8080` (or behind Nginx on port `80`).*

### Step 4: Start the Python Notification Worker
```bash
cd notification-service
pip install pika
python notify.py
```

### Step 5: Unleash the Market Maker (Stress Test)
```bash
cd simulator
pip install requests
python sim.py
```

## ☁️ Kubernetes Deployment (Auto-Scaling)
This system is ready to handle sudden traffic spikes (e.g., Elon Musk tweets). To test the HPA auto-scaling locally (requires Minikube/Docker Desktop K8s):
```bash
kubectl apply -f k8s/exchange-api-deployment.yaml
kubectl apply -f k8s/hpa.yaml
```

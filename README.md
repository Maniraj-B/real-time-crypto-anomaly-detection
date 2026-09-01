# Real-Time Cryptocurrency Market Anomaly Detection Using Multi-Signal Streaming Analytics

> **An End-to-End Kafka, Spark Structured Streaming, HDFS, and Streamlit Pipeline**

A real-time Big Data streaming system that consumes live cryptocurrency trades, processes them using Apache Spark Structured Streaming, calculates interpretable multi-signal anomaly scores, persists historical data in HDFS, and visualizes market behaviour through a live dashboard.

---

## 1. Project Overview

Cryptocurrency markets generate large volumes of high-frequency trading data. Monitoring this data manually makes it difficult to identify short-term periods in which market behaviour becomes statistically unusual.

This project implements an end-to-end streaming analytics pipeline for monitoring **BTCUSDT** and **ETHUSDT** in real time.

The system:

1. Receives live aggregate trades from the Binance public WebSocket.
2. Publishes normalized trade events into Apache Kafka.
3. Processes the Kafka stream using Apache Spark Structured Streaming.
4. Aggregates trades into sliding market windows.
5. Extracts multiple market signals.
6. Calculates an interpretable anomaly score from 0–100.
7. Classifies each window as NORMAL, MEDIUM, HIGH, or CRITICAL.
8. Stores raw trades and analytical results in Apache Hadoop HDFS.
9. Publishes scores and anomaly events back to Kafka.
10. Displays the results through a live Streamlit dashboard.

The system detects **statistically unusual short-term market behaviour**.

It does **not** claim to detect fraud, prove market manipulation, or predict future cryptocurrency prices.

---

## 2. System Architecture

```text
                 Binance Public WebSocket
                  BTCUSDT + ETHUSDT
                           |
                           v
                  +-----------------+
                  | Python Producer |
                  +-----------------+
                           |
                           v
                +---------------------+
                |    Apache Kafka     |
                | crypto.raw.trades   |
                +---------------------+
                           |
                           v
             +----------------------------+
             | Spark Structured Streaming |
             +----------------------------+
                           |
             +-------------+-------------+
             |                           |
             v                           v
       +------------+             +------------------+
       |    HDFS    |             |      Kafka       |
       +------------+             +------------------+
       | raw_trades |             | market.scores    |
       | window_    |             | anomalies        |
       | scores     |             +------------------+
       | checkpoints|                      |
       +------------+                      v
                                   +----------------+
                                   |   Streamlit    |
                                   |   Dashboard    |
                                   +----------------+
```

### Data Flow

```text
Binance
   ↓
Kafka
   ↓
Spark Structured Streaming
   ↓
Multi-Signal Feature Engineering
   ↓
Anomaly Scoring
   ↓
HDFS + Kafka
   ↓
Live Dashboard
```

This provides genuine tool-to-tool Big Data integration rather than running Kafka, Spark, and Hadoop independently.

---

## 3. Technology Stack

### Core Big Data Technologies

| Technology               | Purpose                                            |
| ------------------------ | -------------------------------------------------- |
| Apache Kafka 4.3.1       | Distributed streaming ingestion and output topics  |
| Apache Spark 3.5.9       | Real-time stream processing and analytics          |
| Apache Hadoop HDFS 3.5.0 | Distributed historical data and checkpoint storage |

### Supporting Technologies

| Technology        | Purpose                                |
| ----------------- | -------------------------------------- |
| Python            | Binance producer and application logic |
| Streamlit         | Real-time analytics dashboard          |
| Plotly            | Interactive data visualization         |
| Docker            | Containerized runtime                  |
| Docker Compose    | Multi-service orchestration            |
| Binance WebSocket | Live cryptocurrency market data        |

---

## 4. Live Market Data

The project uses the Binance public aggregate-trade WebSocket.

No Binance account or API key is required.

### Monitored Markets

```text
BTCUSDT
ETHUSDT
```

### Streams

```text
btcusdt@aggTrade
ethusdt@aggTrade
```

### WebSocket

```text
wss://stream.binance.com:9443/stream?streams=btcusdt@aggTrade/ethusdt@aggTrade
```

The producer normalizes incoming Binance events before sending them to Kafka.

Normalized fields include:

```text
event_type
event_time_ms
trade_time_ms
symbol
agg_trade_id
price
quantity
buyer_is_maker
ingested_at
```

---

## 5. Kafka Streaming Layer

Three Kafka topics are used.

```text
crypto.raw.trades
crypto.market.scores
crypto.anomalies
```

### `crypto.raw.trades`

Contains normalized real-time trade events received from Binance.

### `crypto.market.scores`

Contains every completed analytical market window, including normal market conditions.

This topic powers the continuously updating dashboard.

### `crypto.anomalies`

Contains only windows whose anomaly score crosses the effective MEDIUM threshold.

This separates continuous monitoring from threshold-crossing anomaly events.

---

## 6. Spark Structured Streaming

Spark consumes live events directly from:

```text
crypto.raw.trades
```

### Streaming Configuration

```text
Window Size      : 30 seconds
Sliding Interval : 10 seconds
Watermark        : 30 seconds
```

The overlapping sliding windows allow the system to continuously evaluate recent market behaviour rather than waiting for completely separate fixed windows.

---

## 7. Feature Engineering

For every symbol and market window, Spark calculates:

* Trade count
* Mean price
* Price standard deviation
* Minimum price
* Maximum price
* Trading volume
* Notional trading volume
* Buy trade count
* Sell trade count
* Price range percentage
* Coefficient of variation
* Buy/sell imbalance
* Net buy pressure
* Order-flow direction

### Price Range

Measures the magnitude of price movement inside the window.

```text
Price Range % =
(Max Price - Min Price)
-----------------------
       Mean Price
```

### Volatility

Volatility is represented using the coefficient of variation:

```text
Coefficient of Variation =
Price Standard Deviation
------------------------
       Mean Price
```

This normalizes volatility relative to the cryptocurrency's price level.

### Buy/Sell Imbalance

Measures how unequal buy-side and sell-side trade activity is.

Values closer to zero represent more balanced activity.

Larger values indicate stronger order-flow imbalance.

### Net Buy Pressure

The signed order-flow measure determines whether the imbalance is primarily:

```text
BUY_SIDE
```

or:

```text
SELL_SIDE
```

### Trade Activity

The number of trades occurring within the analytical window provides an activity/intensity signal.

---

## 8. Historical Signal Calibration

Instead of using arbitrary fixed denominators, signal components are normalized using historically observed P95 baselines.

### BTCUSDT Baselines

| Signal             |   Baseline |
| ------------------ | ---------: |
| Trade Activity     |       1800 |
| Price Range        |   0.154633 |
| Volatility         | 0.00043187 |
| Buy/Sell Imbalance |   0.796748 |

### ETHUSDT Baselines

| Signal             |   Baseline |
| ------------------ | ---------: |
| Trade Activity     |        863 |
| Price Range        |   0.155301 |
| Volatility         | 0.00044227 |
| Buy/Sell Imbalance |   0.789474 |

Each signal is divided by its corresponding historical baseline and capped at `1.0`.

For example:

```text
Price Component =
min(Current Price Range / Historical Price Range Baseline, 1)
```

These P95 baselines represent historically high **signal intensity**.

They should not be interpreted as anomaly probabilities or percentile probabilities for an individual live window.

---

## 9. Multi-Signal Anomaly Score

The detector combines four normalized market signals.

| Signal             | Weight |
| ------------------ | -----: |
| Price Range        |    35% |
| Volatility         |    30% |
| Buy/Sell Imbalance |    20% |
| Trade Activity     |    15% |

The anomaly score is:

```text
Anomaly Score =
100 × (
    0.35 × Price Component
  + 0.30 × Volatility Component
  + 0.20 × Imbalance Component
  + 0.15 × Activity Component
)
```

The final score ranges from:

```text
0 – 100
```

Because the individual components are retained in each analytical result, the score remains interpretable.

The system also reports the:

```text
dominant_signal
```

responsible for the strongest normalized contribution.

---

## 10. Severity Calibration

Severity thresholds are cryptocurrency-specific.

The production thresholds were derived from historical anomaly-score distributions.

### BTCUSDT

| Severity |           Score |
| -------- | --------------: |
| NORMAL   |       `< 67.20` |
| MEDIUM   | `67.20 – 86.62` |
| HIGH     | `86.63 – 95.52` |
| CRITICAL |      `>= 95.53` |

### ETHUSDT

| Severity |           Score |
| -------- | --------------: |
| NORMAL   |       `< 72.45` |
| MEDIUM   | `72.45 – 85.57` |
| HIGH     | `85.58 – 91.84` |
| CRITICAL |      `>= 91.85` |

The production MEDIUM threshold corresponds to the historical **P90 anomaly-score threshold**.

HIGH corresponds approximately to P95.

CRITICAL corresponds approximately to P99.

Therefore:

> A market window is considered anomalous only after its calibrated score crosses the cryptocurrency-specific historical P90 threshold. Higher severities correspond to the P95 and P99 levels.

---

## 11. Understanding an Anomaly

An anomaly in this project means:

> The combination of observed short-term market signals crossed a statistically calibrated score threshold.

An anomaly does **not** automatically mean:

* Market manipulation
* Fraud
* Illegal trading
* A market crash
* A future price increase
* A future price decrease

The project is a **market-behaviour anomaly detection system**, not a fraud classifier or price forecasting system.

---

## 12. Demo Mode

Real cryptocurrency markets may remain statistically normal during a short classroom demonstration.

For this reason, the project contains a dedicated **Demo Mode**.

### Production Mode

Uses the calibrated thresholds:

```text
BTCUSDT MEDIUM = 67.20
ETHUSDT MEDIUM = 72.45
```

### Demo Mode

Temporarily changes the effective MEDIUM threshold to:

```text
35
```

for both cryptocurrencies.

HIGH and CRITICAL thresholds remain unchanged.

The original production threshold is still preserved in each output payload.

The output therefore contains fields such as:

```text
demo_mode
threshold_mode
production_medium_threshold
medium_threshold
high_threshold
critical_threshold
```

This makes Demo Mode transparent rather than silently modifying the detector.

For presentation purposes:

> For demonstration, the anomaly-event threshold has temporarily been lowered to 35. The production model still uses the statistically calibrated P90 threshold.

### Runtime Configuration

Demo Mode is controlled using:

```text
runtime/demo_mode.json
```

Default:

```json
{
  "demo_mode": false
}
```

The dashboard and Spark share this configuration.

A Spark restart is not required when switching modes.

---

## 13. HDFS Persistence

HDFS stores the project's historical data and Spark streaming checkpoints.

Base directory:

```text
/crypto-anomaly
```

Main directories:

```text
/crypto-anomaly/raw_trades
/crypto-anomaly/window_scores
/crypto-anomaly/checkpoints
```

### `raw_trades`

Historical normalized cryptocurrency trade events.

### `window_scores`

Historical analytical windows and anomaly scores.

### `checkpoints`

Spark Structured Streaming checkpoint state.

Historical analytical data is stored in Parquet format.

---

## 14. Live Dashboard

The Streamlit dashboard provides a real-time interface for monitoring the streaming pipeline.

It includes:

* BTCUSDT monitoring
* ETHUSDT monitoring
* Current anomaly score
* Severity
* Dominant signal
* Price movement
* Volatility
* Trading activity
* Buy/sell imbalance
* Order-flow direction
* Historical score visualization
* Recent anomaly events
* Pipeline status
* Production/Demo Mode state
* Demo Mode toggle

Dashboard timestamps are displayed in **UTC**.

---

# Running the Project

## 15. Requirements

Recommended environment:

```text
Windows 11
WSL2
Docker Desktop
Git
Internet Connection
```

You do **not** need to separately install:

```text
Apache Kafka
Apache Spark
Apache Hadoop
Java
Python
```

The required runtime is containerized with Docker.

---

## 16. Clone the Repository

Open PowerShell.

```powershell
git clone https://github.com/Maniraj-B/real-time-crypto-anomaly-detection.git
```

Enter the project:

```powershell
cd .\real-time-crypto-anomaly-detection
```

---

## 17. Start Docker Desktop

Start Docker Desktop and wait until the Docker engine is running.

WSL2 should be enabled on Windows.

---

## 18. Build and Start

From the project directory:

```powershell
docker compose up -d --build
```

The first startup can take several minutes because Docker may need to download:

* Kafka
* Spark
* Hadoop
* Python dependencies
* Dashboard dependencies

Subsequent startups are normally faster.

---

## 19. Startup Sequence

The Docker Compose configuration automatically coordinates service startup.

```text
Kafka starts
      ↓
Kafka becomes healthy
      ↓
Kafka topics are created

NameNode starts
      ↓
DataNode registers
      ↓
HDFS exits Safe Mode
      ↓
NameNode + DataNode become healthy

      ↓

Producer starts
Spark starts
Dashboard starts
```

Spark waits until HDFS is writable before beginning the streaming queries.

This prevents Spark from attempting to create streaming checkpoints while the NameNode is still in Safe Mode.

---

## 20. Check Service Status

Use:

```powershell
docker compose ps
```

Expected long-running containers include:

```text
crypto-kafka
crypto-namenode
crypto-datanode
crypto-producer
crypto-spark
crypto-dashboard
```

Kafka, NameNode, and DataNode should become healthy.

`crypto-kafka-init` is a one-time initialization service.

It is expected to complete and exit successfully after creating the Kafka topics.

---

## 21. Open the Dashboard

Open:

```text
http://localhost:8501
```

The dashboard should begin displaying live BTCUSDT and ETHUSDT market windows after Spark has accumulated enough streaming data.

---

## 22. Open HDFS

The Hadoop NameNode interface is available at:

```text
http://localhost:9870
```

---

# Verification

## 23. Check the Binance Producer

Run:

```powershell
docker compose logs --tail 30 producer
```

The producer should show a successful Binance WebSocket connection and continuing trade ingestion.

---

## 24. Check Spark

Run:

```powershell
docker compose logs --tail 100 spark
```

Spark should remain running and process incoming Kafka data.

---

## 25. Check HDFS Safe Mode

Run:

```powershell
docker exec crypto-namenode hdfs dfsadmin -safemode get
```

Expected:

```text
Safe mode is OFF
```

---

## 26. Check HDFS Data

Run:

```powershell
docker exec crypto-namenode hdfs dfs -ls /crypto-anomaly
```

Expected directories include:

```text
/crypto-anomaly/checkpoints
/crypto-anomaly/raw_trades
/crypto-anomaly/window_scores
```

---

## 27. Verify Live Kafka Scores

Run:

```powershell
docker exec crypto-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic crypto.market.scores --group manual-score-check --max-messages 4 --timeout-ms 120000
```

The consumer should return live JSON records for BTCUSDT and ETHUSDT.

Example structure:

```json
{
  "symbol": "BTCUSDT",
  "anomaly_score": 42.53,
  "demo_mode": false,
  "threshold_mode": "PRODUCTION",
  "production_medium_threshold": 67.2,
  "medium_threshold": 67.2,
  "high_threshold": 86.63,
  "critical_threshold": 95.53,
  "severity": "NORMAL",
  "dominant_signal": "BUY_SELL_IMBALANCE"
}
```

---

# Stopping and Restarting

## 28. Stop the Project

Use:

```powershell
docker compose down
```

This removes the containers and network while preserving persistent data volumes.

---

## 29. Restart the Project

Use:

```powershell
docker compose up -d
```

The project is designed to recover automatically.

The following are preserved:

* Kafka persisted data
* HDFS historical data
* Spark streaming checkpoints

The pipeline should resume automatically without manually restarting Spark.

---

## 30. Important Data Warning

Do **not** casually use:

```powershell
docker compose down -v
```

The `-v` option removes the project's Docker volumes.

This can delete:

* Kafka persisted topic data
* HDFS NameNode data
* HDFS DataNode data
* Spark checkpoint history stored in HDFS

Use `-v` only when intentionally performing a complete reset.

---

# Fresh Team Installation

## 31. What Happens on a Fresh Clone?

Docker volumes are local to each computer and are **not stored in GitHub**.

Therefore, when another team member clones this repository, their system starts with fresh Kafka and HDFS storage.

This is expected.

On first startup:

```text
GitHub Repository
       ↓
docker compose up -d --build
       ↓
Kafka storage initialized
       ↓
Kafka topics created
       ↓
HDFS NameNode formatted
       ↓
DataNode registered
       ↓
HDFS exits Safe Mode
       ↓
Spark starts
       ↓
Binance producer starts streaming
       ↓
Scores generated
       ↓
HDFS begins collecting local history
       ↓
Dashboard displays live analytics
```

The historically calibrated baseline and severity values are included in the Spark application.

A teammate therefore does not need a copy of the original developer's HDFS volume for the live detector to operate.

---

# Troubleshooting

## 32. Dashboard Does Not Show Data Immediately

Wait approximately 30–90 seconds.

Spark needs sufficient live trades to complete its initial streaming windows.

Check:

```powershell
docker compose ps
```

Then:

```powershell
docker compose logs --tail 50 producer
```

And:

```powershell
docker compose logs --tail 100 spark
```

---

## 33. HDFS Safe Mode

Check:

```powershell
docker exec crypto-namenode hdfs dfsadmin -safemode get
```

Normal operation should show:

```text
Safe mode is OFF
```

The Docker health checks are designed to prevent Spark from starting before this condition is reached.

---

## 34. Spark Checkpoint Recovery

If a stateful Spark query is intentionally changed during development and the existing checkpoint becomes incompatible, the streaming checkpoint may need to be reset.

Stop Spark:

```powershell
docker compose stop spark
```

Delete **only the checkpoint directory**:

```powershell
docker exec crypto-namenode hdfs dfs -rm -r -skipTrash /crypto-anomaly/checkpoints
```

Remove the stopped Spark container:

```powershell
docker compose rm -f spark
```

Start Spark:

```powershell
docker compose up -d spark
```

Do not delete:

```text
/crypto-anomaly/raw_trades
/crypto-anomaly/window_scores
```

for normal checkpoint recovery.

---

## 35. Useful Commands

Service status:

```powershell
docker compose ps
```

Kafka logs:

```powershell
docker compose logs --tail 50 kafka
```

Producer logs:

```powershell
docker compose logs --tail 50 producer
```

Spark logs:

```powershell
docker compose logs --tail 100 spark
```

Dashboard logs:

```powershell
docker compose logs --tail 50 dashboard
```

HDFS directories:

```powershell
docker exec crypto-namenode hdfs dfs -ls /crypto-anomaly
```

---

# Academic Interpretation

## 36. Why This Is a Big Data Project

The system demonstrates a complete streaming Big Data workflow.

### Apache Kafka

Handles real-time ingestion and distributed streaming topics.

### Apache Spark Structured Streaming

Consumes Kafka events, performs windowed aggregation, engineers market features, calculates anomaly scores, and produces analytical results.

### Apache Hadoop HDFS

Provides persistent distributed storage for raw trades, analytical results, and streaming checkpoints.

The tools exchange actual project data:

```text
Kafka → Spark → HDFS
```

while Spark also publishes analytical results back through:

```text
Spark → Kafka → Dashboard
```

---

## 37. Analytical Contribution

The project goes beyond simple price monitoring.

It combines:

```text
Price Movement
       +
Volatility
       +
Order-Flow Imbalance
       +
Trading Activity
       ↓
Multi-Signal Anomaly Score
```

The normalization baselines are historically calibrated, and severity thresholds are cryptocurrency-specific.

This provides a more interpretable approach than assigning arbitrary anomaly thresholds to a single market variable.

---

# Team Responsibilities

## 38. Student 1 — Data Ingestion & Kafka

Responsibilities:

* Binance WebSocket integration
* Python producer
* Event normalization
* Kafka configuration
* Kafka topics
* Streaming ingestion

---

## 39. Student 2 — Spark & Analytics

Responsibilities:

* Spark Structured Streaming
* Sliding windows
* Feature engineering
* Historical calibration
* Multi-signal anomaly scoring
* Severity classification

---

## 40. Student 3 — HDFS, Dashboard & Integration

Responsibilities:

* HDFS storage
* Streaming persistence
* Streamlit dashboard
* Docker integration
* Pipeline testing
* Documentation

All team members should understand the complete architecture and data flow.

---

# Recommended Demonstration

## 41. Demo Sequence

1. Show the complete architecture.
2. Run `docker compose ps`.
3. Show the Binance producer receiving live data.
4. Explain the three Kafka topics.
5. Show Spark Structured Streaming processing.
6. Show HDFS directories and stored data.
7. Open the Streamlit dashboard.
8. Explain the four anomaly-score components.
9. Compare BTC and ETH scores.
10. Explain P90/P95/P99 severity calibration.
11. Enable Demo Mode if the market is quiet.
12. Show a threshold-crossing event.
13. Explain the difference between Demo Mode and Production Mode.
14. Conclude with the complete Kafka → Spark → HDFS integration.

---

# Project Scope

The system should be described as:

> **A real-time multi-signal cryptocurrency market anomaly detection pipeline that identifies statistically unusual short-term market behaviour using Kafka, Spark Structured Streaming, and HDFS.**

The system should not be described as a cryptocurrency fraud detector, manipulation detector, or price prediction engine.

---

## Repository Structure

```text
real-time-crypto-anomaly-detection/
│
├── dashboard/
│   ├── Dockerfile
│   ├── app.py
│   └── requirements.txt
│
├── hadoop/
│   ├── core-site.xml
│   └── hdfs-site.xml
│
├── producer/
│   ├── Dockerfile
│   ├── producer.py
│   └── requirements.txt
│
├── runtime/
│   └── demo_mode.json
│
├── scripts/
│   ├── demo.ps1
│   ├── reset.ps1
│   ├── start.ps1
│   ├── status.ps1
│   └── stop.ps1
│
├── spark/
│   ├── analyze_history.py
│   └── stream_processor.py
│
├── docker-compose.yml
├── .gitignore
└── README.md
```

---

## Current Monitored Markets

```text
BTCUSDT
ETHUSDT
```

---

## Default Runtime Mode

```text
PRODUCTION
```

Demo Mode defaults to:

```text
OFF
```

---

## Final Pipeline

```text
LIVE BINANCE MARKET DATA
          ↓
     APACHE KAFKA
          ↓
SPARK STRUCTURED STREAMING
          ↓
 FEATURE ENGINEERING
          ↓
 MULTI-SIGNAL SCORING
          ↓
 CALIBRATED SEVERITY
          ↓
    ┌─────┴─────┐
    ↓           ↓
   HDFS       KAFKA
                ↓
          LIVE DASHBOARD
```

---

## Project

**Real-Time Cryptocurrency Market Anomaly Detection Using Multi-Signal Streaming Analytics**

**An End-to-End Kafka, Spark Structured Streaming, and HDFS Pipeline**

import json
import os
import signal
import sys
import time
from datetime import datetime, timezone

import websocket
from kafka import KafkaProducer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC = os.getenv("KAFKA_TOPIC", "crypto.raw.trades")
SYMBOLS = [s.strip().lower() for s in os.getenv("SYMBOLS", "btcusdt,ethusdt").split(",") if s.strip()]
WS_BASE = os.getenv("BINANCE_WS_BASE", "wss://stream.binance.com:9443").rstrip("/")

running = True

def shutdown(*_):
    global running
    running = False

signal.signal(signal.SIGTERM, shutdown)
signal.signal(signal.SIGINT, shutdown)

def wait_for_kafka():
    while running:
        try:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
                acks="all",
                retries=10,
                linger_ms=20,
            )
            print(f"[producer] Connected to Kafka at {BOOTSTRAP}")
            return producer
        except Exception as exc:
            print(f"[producer] Kafka not ready: {exc}. Retrying...")
            time.sleep(3)
    return None

def normalize(payload):
    data = payload.get("data", payload)
    # Binance aggregate-trade fields:
    # e event type, E event time, s symbol, a agg trade id,
    # p price, q quantity, T trade time, m buyer-is-maker
    return {
        "event_type": data.get("e", "aggTrade"),
        "event_time_ms": int(data["E"]),
        "trade_time_ms": int(data["T"]),
        "symbol": data["s"],
        "agg_trade_id": int(data["a"]),
        "price": float(data["p"]),
        "quantity": float(data["q"]),
        "buyer_is_maker": bool(data["m"]),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }

def stream_url():
    streams = "/".join(f"{symbol}@aggTrade" for symbol in SYMBOLS)
    return f"{WS_BASE}/stream?streams={streams}"

def main():
    producer = wait_for_kafka()
    if producer is None:
        return

    url = stream_url()
    print(f"[producer] Streaming: {url}")

    while running:
        ws = None
        try:
            ws = websocket.create_connection(url, timeout=30)
            ws.settimeout(60)
            print("[producer] WebSocket connected.")

            count = 0
            while running:
                raw = ws.recv()
                if not raw:
                    continue
                event = normalize(json.loads(raw))
                producer.send(TOPIC, key=event["symbol"], value=event)
                count += 1

                if count % 250 == 0:
                    producer.flush()
                    print(
                        f"[producer] sent={count} latest={event['symbol']} "
                        f"price={event['price']:.4f} qty={event['quantity']:.6f}"
                    )

        except Exception as exc:
            print(f"[producer] WebSocket error: {exc}. Reconnecting in 5s...")
            time.sleep(5)
        finally:
            if ws is not None:
                try:
                    ws.close()
                except Exception:
                    pass

    producer.flush()
    producer.close()
    print("[producer] stopped.")

if __name__ == "__main__":
    main()

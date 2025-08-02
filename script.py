import json
import logging
from datetime import datetime
from dateutil import parser
from influxdb_client.client.write_api import SYNCHRONOUS  # add this at the top
import websocket
from influxdb_client import InfluxDBClient, Point, WritePrecision

from dotenv import load_dotenv
import os


# === Config ===
load_dotenv()

API_KEY = os.getenv("API_KEY")
SECRET_KEY = os.getenv("SECRET_KEY")
ALPACA_URL = os.getenv("ALPACA_URL")

INFLUXDB_URL = os.getenv("INFLUXDB_URL")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET")

# === Logging ===
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# === InfluxDB Setup ===
def setup_influxdb():
    client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
    return client.write_api(write_options=SYNCHRONOUS)

write_api = setup_influxdb()

# ===  Utilities ===
def parse_trade_item(item):
    """Extract fields from a trade item."""
    return {
        "symbol": item["S"],
        "price": item["p"],
        "volume": item.get("s", 0),
        "timestamp": parser.isoparse(item["t"])
    }

def write_to_influx(trade):
    """Write one trade record to InfluxDB."""
    point = (
        Point("stock_trades")
        .tag("symbol", trade["symbol"])
        .field("price", trade["price"])
        .field("volume", trade["volume"])
        .time(trade["timestamp"], WritePrecision.NS)
    )
    write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
    logging.info(f"{trade['symbol']} @ {trade['price']} vol: {trade['volume']}")

# === 🔌 WebSocket Callbacks ===
def on_open(ws):
    logging.info("Connected to Alpaca WebSocket")
    ws.send(json.dumps({
        "action": "auth",
        "key": API_KEY,
        "secret": SECRET_KEY
    }))
    ws.send(json.dumps({
        "action": "subscribe",
        "trades": ["AAPL", "TSLA"]
    }))

def on_message(ws, message):
    data = json.loads(message)
    for item in data:
        if item.get("T") == "t":
            try:
                trade = parse_trade_item(item)
                write_to_influx(trade)
            except Exception as e:
                logging.warning(f"Parse error: {item} — {e}")

def on_error(ws, error):
    logging.error(f"WebSocket error: {error}")

def on_close(ws, code, msg):
    logging.warning(f"WebSocket closed (code: {code}, msg: {msg})")

# === Main Entry ===
if __name__ == "__main__":
    ws = websocket.WebSocketApp(
        ALPACA_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )

    try:
        ws.run_forever(ping_interval=30, ping_timeout=10)
    except KeyboardInterrupt:
        logging.info("Stopped by user. Closing WebSocket.")
        ws.close()

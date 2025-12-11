import django

import json
import time
import websocket

from datetime import datetime, timezone
import time
from django.core.management.base import BaseCommand
from celery import Celery
from tick_consumer.tasks import get_broker, consume_tick

import logging

logger = logging.getLogger(__name__)

# BINANCE WebSocket URL
BINANCE_WS_URL = 'wss://stream.binance.com:9443/stream?streams='


class Command(BaseCommand):
    help = "Runs a tick producer for specific broker"


    def add_arguments(self, parser):
        parser.add_argument("--broker_id", type=int, required=True)

    def handle(self, *args, **options):
        broker_id = options["broker_id"]
        self.stdout.write(self.style.SUCCESS(f"Starting producer for broker {broker_id}"))

        # Fetch broker + scripts via Celery task
        broker_data = fetch_broker_data(broker_id)

        if "error" in broker_data:
            logger.error(f'Received error {broker_data['error']}')
           
        
        # Fetching scripts
        scripts = fetch_broker_scripts(broker_data)

        # Building url
        ws_url = build_WS_URL(scripts)
        self.stdout.write(self.style.SUCCESS(f"Connecting to Binance URL:{ws_url}"))

        # Initiating websocket connection
        initiate_websocket_connection(ws_url, scripts)
        handle_binance_message


# Fetching broker scripts
def fetch_broker_data(broker_id):
    broker_data = get_broker.delay(broker_id).get()
    return broker_data

def fetch_broker_scripts(broker_data):
    scripts = broker_data["scripts"]

    if not scripts:
        logger.error("No scripts found on this broker!!!!")
    return scripts

# Building Binance url
def build_WS_URL(scripts):
    symbols = [s["trading_symbol"].lower() for s in scripts]
    stream_query = "@trade/".join(symbols)
    ws_url = BINANCE_WS_URL + stream_query
    return ws_url

# Initiating binance websocket connection with auto connect
def initiate_websocket_connection(ws_url, scripts):

    # Find script_id from our mapping
    script_map = {s["trading_symbol"]: s["id"] for s in scripts}
    while True:
        try:
            ws = websocket.WebSocketApp(
                ws_url,
                on_message=lambda ws, msg : handle_binance_message(ws, msg ,script_map),
                on_error=on_error, 
                on_close = on_close
                
            )

            ws.run_forever()

        except Exception as e:
            logger.error(f"Websocket crashed:{e}")
            time.sleep(5)
            logger.log("Reconnecting......")

def handle_binance_message(ws, message, script_map):
    try:
        # JSON formatted string.Requires dict conversion
        data = json.loads(message).get("data", {})

        symbol = data.get("s", "").lower()
        price = data.get("p")
        volume = data.get("q")
        ts = data.get("T")

        if not symbol or not price:
            return

        
        if symbol not in script_map:
            return
        
        tick = {
            "script_id": script_map[symbol],
            "value": float(price),
            "volume": float(volume) if volume else None,
            "received_at": datetime.fromtimestamp(ts/1000, tz=timezone.utc).isoformat()
        }


        # Sends ticks to Celery
        consume_tick.delay(tick)


    except Exception as e:
        # Log the error instead of raising
        logger.error(f"Error parsing message: {e}")


def on_error(ws, error):
    print(f"WebSocket Error: {error}")

def on_close(ws, *args):
    print("WebSocket closed. Reconnecting...")

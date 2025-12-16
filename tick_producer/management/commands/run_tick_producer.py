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

# Global counters - Measuring Tick per second
tick_count = 0
last_logged_tick_time = time.time()


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
            self.stdout.write(self.style.ERROR(broker_data["error"]))
            # logger.error(f"{broker_data['error']}")
           
        
        # Fetching scripts
        scripts = fetch_broker_scripts(broker_data)

        # Building url
        ws_url = build_WS_URL(scripts)
        logger.info(f"Connecting to Binance URL:{ws_url}")

        # Initiating websocket connection
        initiate_websocket_connection(ws_url, scripts)

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
    stream_query = "/".join([f"{s['trading_symbol'].lower()}@trade" for s in scripts])
    ws_url = BINANCE_WS_URL + stream_query
    return ws_url

# Initiating binance websocket connection with auto connect
def initiate_websocket_connection(ws_url, scripts):

    # Find script_id from our mapping
    script_map = {s["trading_symbol"]: s["id"] for s in scripts}
    def on_message(ws, msg):
        handle_binance_message(ws, msg ,script_map)

    while True:
        try:
            ws = websocket.WebSocketApp(
                ws_url,
                on_message=on_message,
                on_error=on_error, 
                on_close = on_close
                
            )

            ws.run_forever()

        except Exception as e:
            logger.error(f"Websocket crashed:{e}")
            time.sleep(5)
            logger.log("Reconnecting......")

def handle_binance_message(ws, message, script_map):
    global tick_count, last_logged_tick_time
    start_time = time.time()
    try:
        # JSON formatted string.Requires dict conversion
        data = json.loads(message)["data"]

        symbol = data["s"].lower()
        
        if symbol not in script_map:
            logger.error(f'Stream symbol {symbol} unavailble in map ')

        # Sends ticks to Celery
        consume_tick.delay({
            "script_id": script_map[symbol],
            "value": data["p"],
            "volume": data["q"] if data["q"] else None,
            "received_at": data["T"]
        })
        current_time = time.time()
        tick_processing_time = current_time - start_time

        tick_count +=1 
        time_diff = current_time - last_logged_tick_time

        if  time_diff >= 1.0:
            ticks_per_second = tick_count/time_diff
            print(f" Processing time: {tick_processing_time * 1000} ms | Ticks production rate: {ticks_per_second:.2f} ticks/sec| Total ticks = {tick_count} ticks")
            tick_count = 0
            last_logged_tick_time = current_time


    except Exception as e:
        # Log the error instead of raising
        logger.error(f"Error parsing message: {e}")

def on_error(ws, error):
    logger.error(f"WebSocket Error: {error}")

def on_close(ws, *args):
    logger.info("WebSocket closed. Reconnecting...")

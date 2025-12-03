import django

import json
import time
import websocket

from datetime import datetime
from django.core.management.base import BaseCommand
from celery import Celery
from tick_consumer.tasks import get_broker, consume_tick

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
        broker_data = get_broker.delay(broker_id).get()

        if "error" in broker_data:
            self.stdout.write(self.style.ERROR(broker_data["error"]))
            return
        
        scripts = broker_data["scripts"]

        if not scripts:
            self.stdout.write(self.style.WARNING("No scripts found on this broker!!!!"))
            return 
        

        # Build Binance subscription URL
        symbols = [s["trading_symbol"].lower() for s in scripts]
        stream_query = "/".join([f"{sym}@trade" for sym in symbols])
        ws_url = BINANCE_WS_URL + stream_query


        self.stdout.write(self.style.SUCCESS(f"Connecting to Binance URL:{ws_url}"))


        # Performing auto connect
        while True:
            try:
                ws = websocket.WebSocketApp(
                    ws_url,
                    on_message=lambda ws, msg: self.on_message(ws, msg, scripts),
                    on_error=self.on_error,
                    on_close=self.on_close
                )

                ws.run_forever()

            except Exception as e:
                self.stdout.write(self.style.ERROR(f"Websocket crashed:{e}"))
                time.sleep(5)
                self.stdout.write(self.style.WARNING("Reconnecting......"))



    def on_message(self, ws, message, scripts):
        try:
            data = json.loads(message)

            # Binance trade stream payload
            trade = data.get("data", {})

            symbol = trade.get("s", "").lower()
            price = trade.get("p")
            volume = trade.get("q")
            ts = trade.get("T")

            if not symbol or not price:
                return

            # Find script_id from our mapping
            script_map = {s["trading_symbol"]: s["id"] for s in scripts}
            if symbol not in script_map:
                return
            
            tick = {
                "script_id": script_map[symbol],
                "value": float(price),
                "volume": float(volume) if volume else None,
                "received_at": datetime.utcfromtimestamp(ts / 1000).isoformat()
            }

            # Sends ticks to Celery
            consume_tick.delay([tick])


        except Exception as e:
            print(f"Error parsing message: {e}")

    def on_error(self, ws, error):
        print(f"WebSocket Error: {error}")

    def on_close(self, ws, *args):
        print("WebSocket closed. Reconnecting...")
            
        
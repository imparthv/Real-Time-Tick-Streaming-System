from celery import shared_task

from .models import Broker, Ticks
from datetime import datetime, timezone
import time
import logging

# Creating a logger
logger = logging.getLogger(__name__)

# try: 
#     from line_profiler import profile # type: ignore
# except ImportError:
#     def profile(func):
#         return func


# Returns broker details and all the scripts related to broker
# The scripts are list of dictionaries
@shared_task
def get_broker(broker_id):
    try:
        broker = Broker.objects.get(id=broker_id)
        scripts = broker.scripts.all()
        scripts_data = [
            {
                'id': script.id,
                'name': script.name,
                'trading_symbol': script.trading_symbol,
                'additional_data': script.additional_data
            }
            for script in scripts
        ]

        return {
            'id': broker.id,
            'type': broker.type,
            'name': broker.name,
            'api_config': broker.api_config,
            'scripts': scripts_data
        }

    except Broker.DoesNotExist:
        # Logging error instead of raising
        logger.error(f'Broker with id {broker_id} not found') 
    

# Accepts a single tick
@shared_task
# @profile
def consume_tick(tick_data):
    try:
        # received_at = parse_datetime(tick_data["received_at"])
        start_time = time.perf_counter()
        Ticks.objects.create(
                script_id = tick_data["script_id"],
                tick_value = tick_data["value"],
                volume = tick_data["volume"],
                received_at_producer = datetime.fromtimestamp(tick_data["received_at"]/1000, tz=timezone.utc).isoformat()
        )
        elasped_time = (time.perf_counter() -  start_time)
        logger.info(f"Inserted a tick in {elasped_time * 1000} ms")
        # profile.print_stats()

    except Exception as e:
        # Log the error instead of raising
        logger.error(f"Error with tick: {tick_data} - {e}")
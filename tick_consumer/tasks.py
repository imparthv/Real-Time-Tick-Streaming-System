from celery import shared_task
from django.core.serializers import serialize

from .models import Broker, Script,Ticks
from django.utils.dateparse import parse_datetime
from django.db import transaction


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
        raise {'error': f'Broker with id {broker_id} not found'} 
    

# Accepts list of tick dictionaries
@shared_task
def consume_tick(tick_data_list):
    ticks_to_create = []
    for tick in tick_data_list:
        try:
            received_at = parse_datetime(tick["received_at"])
            ticks_to_create.append(
                Ticks(
                    script_id = tick["script_id"],
                    tick_value = tick["value"],
                    volume = tick["volume"],
                    received_at_producer = received_at
                )
                )
        except Exception as e:
            print(f"Error parsing tick: {tick} - {e}")

    if ticks_to_create:
        try:
            # Ensuring atomic transactions: Either the all changes are made upon success 
            # Or the changes are rolled back upon failure
            with transaction.atomic():
                # Ensuring buld inserts into tick table
                Ticks.objects.bulk_create(ticks_to_create)
            print(f"Inserted {len(ticks_to_create)} ticks")

        except Exception as e:
            print(f"Error inserting tick: {e}")


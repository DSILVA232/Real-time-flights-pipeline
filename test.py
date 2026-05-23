from confluent_kafka import Producer
from pydantic import BaseModel, ValidationError
from typing import Optional
import requests
import json
import time

producer = Producer({'bootstrap.servers': 'localhost:9092'})

base_url = "https://opensky-network.org/api"

with open('country_Bboxes.json', 'r') as file:
    data = json.load(file)

country_data = data["Australia"]
lamin = country_data["lamin"]
lomin = country_data["lomin"]
lamax = country_data["lamax"]
lomax = country_data["lomax"]


class location_FlightEvent(BaseModel):
    longitude: Optional[float]
    latitude: Optional[float]
    baro_altitude: Optional[float]
    geo_altitude: Optional[float]


class velocity_FlightEvent(BaseModel):
    velocity: Optional[float]
    true_track: Optional[float]
    vertical_rate: Optional[float]

class identity_FlightEvent(BaseModel):
    icao24: str
    callsign: Optional[str]
    origin_country: Optional[str]

class timing_FlightEvent(BaseModel):
    time_position: Optional[int]
    last_contact: Optional[int]


class operational_FlightEvent(BaseModel):
    on_ground: bool
    squawk: Optional[str]
    spi: Optional[bool]
    position_source: [int]




columns = [
    "icao24", "callsign", "origin_country", "time_position", "last_contact",
    "longitude", "latitude", "baro_altitude", "on_ground", "velocity",
    "true_track", "vertical_rate", "sensors", "geo_altitude", "squawk", "spi", "position_source"
]

def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}]")

def poll_and_publish():
    try:
        response = requests.get(
            f"{base_url}/states/all?lamin={lamin}&lomin={lomin}&lamax={lamax}&lomax={lomax}",
            timeout=10
        )
        response.raise_for_status()

        flight_data = response.json()
        states = flight_data.get("states", [])

        published = 0
        skipped = 0

        for state in states:
           
            raw = dict(zip(columns, state))

            try:
                event = FlightEvent(**raw)

                producer.produce(
                    topic="flight_status",
                    key=event.icao24,          
                    value=event.model_dump_json().encode("utf-8"),
                    callback=delivery_report
                )
                producer.produce(
                    topic="flight_status",
                    key=event.icao24,          
                    value=event.model_dump_json().encode("utf-8"),
                    callback=delivery_report
                )
                producer.produce(
                    topic="flight_status",
                    key=event.icao24,          
                    value=event.model_dump_json().encode("utf-8"),
                    callback=delivery_report
                )
                producer.produce(
                    topic="flight_status",
                    key=event.icao24,          
                    value=event.model_dump_json().encode("utf-8"),
                    callback=delivery_report
                )
                published += 1

            except ValidationError as e:
                skipped += 1
                print(f"Skipping malformed event {raw.get('icao24')}: {e}")

        producer.flush()
        print(f"Batch complete — published: {published}, skipped: {skipped}")

    except Exception as e:
        print(f"Poll failed: {e}")



poll_and_publish()

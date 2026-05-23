from confluent_kafka import Producer
from pydantic import BaseModel, ValidationError
from typing import List,Optional
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


class FlightEvent(BaseModel):
    icao24: str
    callsign: Optional[str]
    origin_country: Optional[str]
    time_position: Optional[int]
    last_contact: Optional[int]
    longitude: Optional[float]
    latitude: Optional[float]
    baro_altitude: Optional[float]
    on_ground: bool
    velocity: Optional[float]
    true_track: Optional[float]
    vertical_rate: Optional[float]
    sensors: Optional[List[any]]
    geo_altitude: Optional[float]
    squawk: Optional[str]
    spi: Optional[bool]
    position_source: Optional[int]

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
                    topic="flight_identity",
                    key=event.icao24 ,          
                    value=json.dumps({
                        "icao24": event.icao24,
                        "callsign": event.callsign,
                        "origin_country": event.origin_country
                       }),
                    callback=delivery_report
                )

                producer.produce(
                    topic="flight_position",
                    key=event.icao24 ,          
                    value=json.dumps({
                        "icao24": event.icao24,
                        "longitude": event.longitude,
                        "baro_altitude": event.baro_altitude
                       }),
                    callback=delivery_report
                )

                producer.produce(
                    topic="flight_motion",
                    key=event.icao24 ,          
                    value=json.dumps({
                        "icao24": event.icao24,
                        "velocity": event.velocity,
                        "true_track": event.true_track,
                        "vertical_rate": event.vertical_rate
                       }),
                    callback=delivery_report
                )


                producer.produce(
                    topic="flight_status",
                    key=event.icao24 ,          
                    value=json.dumps({
                        "icao24": event.icao24,
                        "on_ground": event.on_ground,
                        "squawk": event.squawk,
                        "spi": event.spi,
                        "position_source": event.position_source
                       }),
                    callback=delivery_report
                )

                producer.produce(
                    topic="flight_timing",
                    key=event.icao24 ,          
                    value=json.dumps({
                        "icao24": event.icao24,
                        "time_position": event.time_position,
                        "last_contact": event.last_contact
                       }),
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

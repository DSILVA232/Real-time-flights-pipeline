# Kafka + OpenSky + Pydantic (Summary Notes)

This was a learning expirience for me so i tried to document it as much as i could in the way i came to understand things and build a bigger picture, by combining reading documentation and pre-existing python knowledge

## 1. Pipeline Overview

```text
OpenSky API
  ↓
states (list of aircraft)
  ↓
for each aircraft
  ↓
dict (raw)
  ↓
Pydantic model (FlightEvent)
  ↓
JSON string
  ↓
Kafka topic
```

---

## 2. Key Steps

### 1. API Response

* `states` = list of aircraft
* Each item = one aircraft (list)

---

### 2. List → Dict

```python
raw = dict(zip(columns, state))
```

* Converts list → named fields
* Access: `raw["icao24"]`

---

### 3. Dict → Pydantic Object

```python
event = FlightEvent(**raw)
```

* `**raw` unpacks dict into arguments
* Validates types + structure
* Creates object with attributes

Access:

```python
event.icao24
```

---

### 4. JSON Conversion

#### Option A (recommended)

```python
event.model_dump_json()
```

* Object → JSON string automatically

#### Option B

```python
json.dumps({"icao24": event.icao24})
```

* Manually build dict → JSON string

---

### 5. Kafka Producer

```python
producer.produce(
    topic="flight_status",
    key=event.icao24,
    value=event.model_dump_json()
)
```

* `key` = aircraft ID (grouping)
* `value` = JSON string
* Kafka stores bytes only

---

## 3. Core Concepts

### What is what?

* `states` → list of aircraft
* `raw` → dictionary (key-value data)
* `event` → Pydantic object
* Kafka message → JSON string

---

## 4. Data Flow (Important)

```text
API list
  ↓
Dict (raw)
  ↓
Pydantic object
  ↓
JSON string
  ↓
Kafka message
```

---

## 5. Key Ideas

* `**raw` = unpack dict into class
* `event.icao24` = object attribute access
* `json.dumps()` = dict → JSON string
* Kafka stores only serialized bytes
* 1 aircraft = 1 Kafka message

---

## 6. Most Important Insight

Kafka does NOT store the API response.

It stores individual aircraft events created in the loop.

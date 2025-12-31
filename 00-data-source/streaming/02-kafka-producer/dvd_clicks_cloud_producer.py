import json
import sys
import csv
import random
import uuid
from datetime import datetime, timezone, timedelta
import time
from confluent_kafka import Producer

topic = "dvd_clicks"

# Keep event types simple + usable in dashboards
event_types = {
    "view": 0.35,
    "detail_view": 0.30,
    "watch_trailer": 0.20,
    "add_to_watchlist": 0.15
}

devices = ["web", "mobile", "tablet"]
referrers = ["homepage", "search", "recommendation"]

SESSION_TIMEOUT_MIN = 10  # reuse a session for 10 mins of inactivity

def read_ccloud_config(config_file):
    conf = {}
    with open(config_file) as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split("=", 1)
                conf[parameter] = value.strip()
    return conf

def delivery_callback(err, msg):
    if err:
        sys.stderr.write(f"%% Message failed delivery: {err}\n")
    else:
        sys.stderr.write(
            f"%% Message delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}\n"
        )

# ---------------------------
# Load films from CSV (film_id, film_title)
# ---------------------------
def load_films(csv_path: str) -> list[dict]:
    films = []
    try:
        with open(csv_path, encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            # Expect columns: film_id, film_title
            for row in reader:
                film_id = row.get("film_id")
                film_title = row.get("film_title")
                if film_id and film_title:
                    films.append({"film_id": int(film_id), "film_title": film_title.strip()})

        print(f"Loaded {len(films)} films from CSV")

    except FileNotFoundError:
        print(f"❌ CSV file not found: {csv_path}")
    except Exception as e:
        print(f"❌ Error loading CSV file: {e}")

    return films

def iso_utc_now_ms() -> str:
    # ISO8601 UTC with milliseconds and Z suffix
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

def pick_event_type() -> str:
    return random.choices(
        population=list(event_types.keys()),
        weights=list(event_types.values()),
        k=1
    )[0]

def page_for_event(event_type: str) -> str:
    return {
        "view": "browse",
        "detail_view": "film_detail",
        "watch_trailer": "film_detail",
        "add_to_watchlist": "film_detail",
    }.get(event_type, "browse")

# ---------------------------
# Main producer logic
# ---------------------------
def start_producer(csv_path="./csv/films.csv") -> None:
    films = load_films(csv_path)
    if not films:
        raise Exception("No films loaded (need film_id + film_title). Exiting.")

    producer = Producer(read_ccloud_config("client.properties"))

    # Track per-user session reuse
    user_sessions: dict[str, dict] = {}  # user_id -> {"session_id": str, "last_seen": datetime}

    print("Kafka producer initialized. Streaming started...")
    print("Press CTRL+C to stop.\n")

    try:
        while True:
            event_id = str(uuid.uuid4())

            user_id = f"u_{random.randint(1, 10)}"
            now = datetime.now(timezone.utc)

            # Session reuse with timeout
            sess = user_sessions.get(user_id)
            if (not sess) or (now - sess["last_seen"] > timedelta(minutes=SESSION_TIMEOUT_MIN)):
                session_id = f"s_{uuid.uuid4().hex[:8]}"
            else:
                session_id = sess["session_id"]

            user_sessions[user_id] = {"session_id": session_id, "last_seen": now}

            film = random.choice(films)
            event_type = pick_event_type()

            message = {
                "event_id": event_id,
                "event_ts": iso_utc_now_ms(),
                "user_id": user_id,
                "session_id": session_id,
                "film_id": film["film_id"],
                "event_type": event_type,
                "page": page_for_event(event_type),
                "position": random.randint(1, 20),
                "device": random.choice(devices),
                "referrer": random.choice(referrers)
            }

            # Key by user_id (best for session/user analytics)
            producer.produce(
                topic=topic,
                key=json.dumps(user_id).encode("utf-8"),
                value=json.dumps(message).encode("utf-8"),
                callback=delivery_callback
            )
            producer.poll(0)

            print(f"Produced event for user={user_id} session={session_id} event_type={event_type}  film_id={film['film_id']}")

            time.sleep(random.uniform(0.5, 1.5))

    except KeyboardInterrupt:
        print("\nStopping producer...")

    finally:
        producer.flush()
        print("Producer closed.")

if __name__ == "__main__":
    start_producer()

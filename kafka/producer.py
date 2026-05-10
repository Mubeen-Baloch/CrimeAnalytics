import argparse
import csv
import glob
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

import yaml
from kafka import KafkaProducer


REQUIRED_OUTPUT_FIELDS = {
    "case_number": ["Case Number", "CASE NUMBER", "case_number"],
    "date": ["Date", "DATE", "date"],
    "block": ["Block", "BLOCK", "block"],
    "primary_type": ["Primary Type", "PRIMARY TYPE", "primary_type"],
    "district": ["District", "DISTRICT", "district"],
    "arrest": ["Arrest", "ARREST", "arrest"],
    "latitude": ["Latitude", "LATITUDE", "latitude"],
    "longitude": ["Longitude", "LONGITUDE", "longitude"],
}


def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def resolve_one(pattern: str) -> str:
    matches = sorted(glob.glob(pattern))
    if not matches:
        raise FileNotFoundError(f"No files matched pattern: {pattern}")
    return matches[0]


def pick(row: dict, names: list[str]) -> str | None:
    for name in names:
        if name in row:
            return row.get(name)
    return None


def normalize_event(row: dict) -> dict:
    event = {key: pick(row, names) for key, names in REQUIRED_OUTPUT_FIELDS.items()}
    missing = [key for key, value in event.items() if value in (None, "")]
    if missing:
        raise ValueError(f"missing required fields: {', '.join(missing)}")

    event["arrest"] = str(event["arrest"]).strip().lower() in {"true", "t", "1", "yes", "y"}
    event["district"] = str(event["district"]).strip().zfill(3)
    event["latitude"] = float(event["latitude"])
    event["longitude"] = float(event["longitude"])
    event["produced_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    return event


def build_producer(bootstrap_servers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda value: value.encode("utf-8") if value else None,
        linger_ms=50,
        retries=5,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay Chicago crime CSV rows to Kafka.")
    parser.add_argument("--config", default="config/config.yaml")
    parser.add_argument("--max-rows", type=int, default=None, help="Override config producer_max_rows for smoke tests.")
    parser.add_argument("--rate", type=float, default=None, help="Override config producer_rate_per_second.")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    config = load_config(args.config)
    local_pattern = config["data"]["crime_glob"].replace("/app/", "")
    crime_path = resolve_one(config["data"]["crime_glob"] if Path("/app").exists() else local_pattern)
    topic = config["kafka"]["topic"]
    bootstrap_servers = config["kafka"]["bootstrap_servers"]
    if bootstrap_servers == "kafka:9092" and not Path("/app").exists():
        bootstrap_servers = "localhost:29092"

    rate = args.rate if args.rate is not None else float(config["kafka"].get("producer_rate_per_second", 1.0))
    max_rows = args.max_rows if args.max_rows is not None else int(config["kafka"].get("producer_max_rows", 0))
    sleep_seconds = 0 if rate <= 0 else 1.0 / rate

    logging.info("Streaming %s to topic %s through %s", crime_path, topic, bootstrap_servers)
    producer = build_producer(bootstrap_servers)
    sent = 0
    skipped = 0

    with open(crime_path, "r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row_number, row in enumerate(reader, start=2):
            try:
                event = normalize_event(row)
                producer.send(topic, key=event["district"], value=event)
                sent += 1
                if sent % 1000 == 0:
                    producer.flush()
                    logging.info("Sent %s events; skipped %s", f"{sent:,}", f"{skipped:,}")
            except Exception as exc:
                skipped += 1
                logging.warning("Skipping row %s: %s", row_number, exc)

            if max_rows and sent >= max_rows:
                break
            if sleep_seconds:
                time.sleep(sleep_seconds)

    producer.flush()
    logging.info("Finished. Sent %s events; skipped %s", f"{sent:,}", f"{skipped:,}")


if __name__ == "__main__":
    main()

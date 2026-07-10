import csv
import random
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd


def base_data_for_csv():
    return {"SKUS": [f"SKU-{n:04d}" for n in range(1, 21)],
     "WAREHOUSES": ["WH-POZNAN", "WH-WARSZAWA"],
     "EVENT_TYPES_IN": ["receipt", "return"],
     "EVENT_TYPES_OUT": ["pick", "shipment", "damage"]}


old_file_path = "movements_large.csv"

def event_id_continuity(existing_csv_path):
    """
    Calculates the event_id to start the generation with depending on where the previous file left off
    Accounts for first time file creation, starts with event_id at 0
    """
    if Path(existing_csv_path).exists():
        with open(existing_csv_path, encoding='utf-8') as f:
            reader = csv.DictReader(f)
            max_existing_id = max(int(row['event_id']) for row in reader)
            print(f"Max existing id: {max_existing_id}")
        return max_existing_id
    else:
        return 0

def timestamp_continuity(existing_csv_path):
    """
    Reads the oldest timestamp in the previouly used file, returns it.
    On first use, provides a base date.
    """
    if Path(existing_csv_path).exists():
        df = pd.read_csv(existing_csv_path, usecols=["event_timestamp"])
        return str(df.iloc[-1, 0])
    else:
        return "2024-01-01 00:00:00"


def start_date(timestamp_function=timestamp_continuity):
    """Converts the str output from timestamp contituity to datetime object"""
    parsed_date = timestamp_function(existing_csv_path=old_file_path).split(" ")[0].split("-")
    print(f"Most recent timestamp found in old file: {timestamp_function(existing_csv_path=old_file_path)}")
    return datetime(int(parsed_date[0]), int(parsed_date[1]), int(parsed_date[2])) + timedelta(days=1)


def generate_movements(num_events, days_span, output_path, starting_event_id, start_timestamp):
    """
    num_events: total rows to generate
    days_span: how many simulated days the events are spread across
    """

    rows = []

    def event_id_assignment_logic():
        if Path("movements_large.csv").exists():
            return starting_event_id + 1
        else:
            return starting_event_id

    event_id = event_id_assignment_logic()

    print(f"Event id: {event_id}")

    current_stock = {sku: random.randint(50, 200) for sku in base_data_for_csv()["SKUS"]}

    for _ in range(num_events):
        sku = random.choice(base_data_for_csv()["SKUS"])
        warehouse = random.choice(base_data_for_csv()["WAREHOUSES"])

        #SKU generation logic, determining an outbound bias
        if random.random() < 0.7 and current_stock[sku] > 5:
            event_type = random.choice(base_data_for_csv()["EVENT_TYPES_OUT"])
            quantity = min(random.randint(1, 15), current_stock[sku])
            current_stock[sku] -= quantity
        else:
            event_type = random.choice(base_data_for_csv()["EVENT_TYPES_IN"])
            quantity = random.randint(5, 50)
            current_stock[sku] += quantity

        # Spread timestamps realistically across the span, business hours only
        day_offset = random.uniform(0, days_span)
        hour = random.randint(6, 18)
        timestamp = start_timestamp + timedelta(days=day_offset, hours=hour,
                                            minutes=random.randint(0, 59))

        rows.append({
            "event_id": event_id,
            "sku": sku,
            "warehouse_id": warehouse,
            "event_type": event_type,
            "quantity": quantity,
            "event_timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
        })
        event_id += 1

    rows.sort(key=lambda r: r["event_timestamp"])

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {len(rows)} events spanning {days_span} days to {output_path}")



if __name__ == "__main__":
    generate_movements(
        num_events=500_000,
        days_span=180,
        output_path="movements_large.csv",
        starting_event_id=event_id_continuity(old_file_path),
        start_timestamp = start_date()
    )
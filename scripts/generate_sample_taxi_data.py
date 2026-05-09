import csv
import os
import random
from datetime import datetime, timedelta

output_dir = "data/raw"
os.makedirs(output_dir, exist_ok=True)
output_file = os.path.join(output_dir, "taxi_trips_sample.csv")

start = datetime(2024, 1, 1, 0, 0, 0)

with open(output_file, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow([
        "tpep_pickup_datetime",
        "trip_distance",
        "fare_amount"
    ])

    for _ in range(5000):
        pickup = start + timedelta(minutes=random.randint(0, 60 * 24 * 31 - 1))
        trip_distance = round(random.uniform(0.5, 25.0), 2)
        fare_amount = round(3.0 + trip_distance * random.uniform(1.5, 3.0), 2)

        writer.writerow([
            pickup.strftime("%Y-%m-%d %H:%M:%S"),
            trip_distance,
            fare_amount
        ])

print(f"Generated file: {output_file}")

from __future__ import annotations

import csv
import json
from pathlib import Path

from faker import Faker
from func_perf import timeit, timeit_stats  # assuming your decorators are in func_perf.py


def make_fake_people(n: int, *, locale: str = "en_US", seed: int | None = None):
    fake = Faker(locale)
    if seed is not None:
        # Global seeding is fine for reproducible sequences in simple scripts
        Faker.seed(seed)

    records = []
    for _ in range(n):
        company = fake.company()
        record = {
            "name": fake.name(),
            "email": fake.email(),
            "phone": fake.phone_number(),
            "company": company,
            "role": fake.job(),
        }
        records.append(record)
    return records


@timeit  # measure generation time
def main():
    return make_fake_people(50000, locale="en_US", seed=200)


@timeit  # measure CSV write time
def save_to_csv(records: list[dict], path: Path) -> None:
    # Ensure directory exists
    path.parent.mkdir(parents=True, exist_ok=True)

    # Use newline="" to avoid blank lines on Windows; UTF-8 for broad character support
    fieldnames = ["name", "email", "phone", "company", "role"]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)


@timeit  # measure JSON write time
def save_to_json(records: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        # indent for readability; ensure_ascii=False keeps non-ASCII characters intact
        json.dump(records, f, indent=2, ensure_ascii=False)


if __name__ == "__main__":
    data = main()

    out_dir = Path("out")
    csv_path = out_dir / "people.csv"
    json_path = out_dir / "people.json"

    save_to_csv(data, csv_path)
    save_to_json(data, json_path)

    print(f"Wrote {len(data)} records to:\n- {csv_path}\n- {json_path}")

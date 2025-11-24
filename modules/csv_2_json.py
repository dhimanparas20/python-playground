import csv
from typing import List,Dict,Any

def csv_to_json_list(csv_filename: str) -> List[Dict[str, Any]]:
    try:
        # Read the CSV and convert to list of dicts
        print(f"Reading CSV file: {csv_filename}")
        with open(csv_filename, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            data = [row for row in reader]
        print(f"Read {len(data)} rows from CSV file: {csv_filename}")
        return data[1:]
    except Exception as e:
        print(f"Error removing file {csv_filename}: {e}")
        return []


csv_to_json_list("")
#!/usr/bin/env python3
import csv
import re
from collections import Counter

# Path to your CSV file
csv_file = "logs.csv"

# Regular expression to extract numeric user ID
user_id_pattern = re.compile(r"/userinit request for (\d+)")

# List to collect user IDs
user_ids = []

# Read the CSV and extract user IDs
with open(csv_file, 'r', encoding='utf-8') as file:
    reader = csv.DictReader(file)
    for row in reader:
        message = row.get("message", "")
        match = user_id_pattern.search(message)
        if match:
            user_ids.append(match.group(1))

# Count occurrences
user_id_counts = Counter(user_ids)

# Find duplicates
duplicates = {user_id: count for user_id, count in user_id_counts.items() if count > 1}

# Output results
print(f"Total number of user ids found is {len(user_id_counts.items())}")
if duplicates:
    print("Duplicate user IDs found:")
    for user_id, count in duplicates.items():
        print(f"User ID {user_id} appears {count} times")
else:
    print("No duplicate user IDs found.")

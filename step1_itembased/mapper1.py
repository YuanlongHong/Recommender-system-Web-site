#!/usr/bin/env python3
import sys

# --- Item-Based Mapper 1 ---
# Goal: Group by UserID
# Input: userId, movieId, rating
# Output: userId \t movieId:rating

for line in sys.stdin:
    line = line.strip()
    # Skip empty lines or headers
    if not line or not line[0].isdigit():
        continue

    parts = line.split(',')
    if len(parts) < 3:
        continue

    # CSV Format: userId, movieId, rating
    user = parts[0]
    movie = parts[1]
    rating = parts[2]

    #group by USER, not Movie
    # Output: Key=User, Value=Movie:Rating
    print(f"{user}\t{movie}:{rating}")
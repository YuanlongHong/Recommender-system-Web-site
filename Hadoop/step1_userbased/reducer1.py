#!/usr/bin/env python3
import sys
import random
from itertools import combinations

current_movie = None
user_ratings = []

# Safety threshold: skip movies with too many users to avoid combinatorial explosion
MAX_USERS_PER_MOVIE = 200


def process_movie(movie, user_ratings):
    """
    Process all user-rating pairs for a single movie.
    Generate all user-user rating pairs (co-rating pairs).
    Skip the movie if too many users rated it (to avoid O(n^2) explosion).
    """
    n = len(user_ratings)
    if n < 2:
        return

    # Skip extremely popular movies to prevent reducer overload
    if n > MAX_USERS_PER_MOVIE:
        user_ratings = random.sample(user_ratings, MAX_USERS_PER_MOVIE)

    # Emit all pairwise combinations of user ratings
    for a, b in combinations(user_ratings, 2):
        # Safety check: avoid malformed input
        if ":" not in a or ":" not in b:
            continue
        u1, r1 = a.split(':', 1)
        u2, r2 = b.split(':', 1)
        print(f"{u1},{u2}\t{r1},{r2}")


for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    if "\t" not in line:
        continue

    movie, ur = line.split("\t", 1)

    # Skip malformed input line
    if ":" not in ur:
        continue

    # First record
    if current_movie is None:
        current_movie = movie

    # New movie group → process previous movie
    if movie != current_movie:
        process_movie(current_movie, user_ratings)
        user_ratings = []
        current_movie = movie

    user_ratings.append(ur)

# Process last movie
if current_movie is not None:
    process_movie(current_movie, user_ratings)
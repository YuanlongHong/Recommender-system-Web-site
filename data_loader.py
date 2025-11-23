# data_loader.py
import pymysql
from collections import defaultdict

# MySQL connection config
DB_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "your password",
    "database": "cse482",
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor,
}


def get_connection():
    return pymysql.connect(**DB_CONFIG)  # new MySQL connection


def load_movies():
    movies = {}

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT movieId, title, genres FROM movies")
            rows = cur.fetchall()

        for row in rows:
            mid = int(row["movieId"])
            title = row["title"]
            genres = row["genres"] or ""  
            movies[mid] = {"title": title, "genres": genres}
    finally:
        conn.close()

    return movies  # {movieId: {"title": ..., "genres": ...}}


def load_ratings():
    user_ratings = defaultdict(list)  # {userId: [(movieId, rating), ...]}

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT userId, movieId, rating "
                "FROM ratings_train"
            )
            rows = cur.fetchall()

        for row in rows:
            uid = int(row["userId"])
            mid = int(row["movieId"])
            r = float(row["rating"])
            user_ratings[uid].append((mid, r))
    finally:
        conn.close()

    return user_ratings


def load_neighbors(topk=50):
    user_neighbors = {}  # {userId: [(neighborId, similarity), ...]}

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT userId, neighbors FROM user_topk_neighbors")
            rows = cur.fetchall()

        for row in rows:
            uid = int(row["userId"])
            neigh_line = row["neighbors"] or "" 
            pairs = []
            for item in neigh_line.split(","):
                item = item.strip()
                if not item or ":" not in item:
                    continue
                nid_str, sim_str = item.split(":", 1)
                try:
                    nid = int(nid_str)
                    sim = float(sim_str)
                except ValueError:
                    continue
                pairs.append((nid, sim))

            # optional: enforce top-k cutoff
            if topk is not None and len(pairs) > topk:
                pairs = pairs[:topk]

            user_neighbors[uid] = pairs
    finally:
        conn.close()

    return user_neighbors


def load_recommendations():
    user_recs = defaultdict(list)  # {userId: [(movieId, predicted_rating), ...]}

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT userId, movieId, prediction "
                "FROM user_item_predictions "
                "ORDER BY userId, prediction DESC"  # sorted by score within each user
            )
            rows = cur.fetchall()

        for row in rows:
            uid = int(row["userId"])
            mid = int(row["movieId"])
            pred = float(row["prediction"])
            user_recs[uid].append((mid, pred))
    finally:
        conn.close()

    return user_recs


def load_all_data():
    movies = load_movies()
    user_ratings = load_ratings()
    user_neighbors = load_neighbors()
    user_recs = load_recommendations()

    # all users that appear in either ratings or recommendations
    all_users = sorted(set(user_ratings.keys()) | set(user_recs.keys()))

    return {
        "movies": movies,
        "user_ratings": user_ratings,
        "user_neighbors": user_neighbors,
        "user_recs": user_recs,
        "all_users": all_users,
    }
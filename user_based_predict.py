import csv
import time
from collections import defaultdict
from multiprocessing import Pool

RATINGS_FILE = "ratings_train.csv"
USER_NEIGHBORS_FILE = "user_topk_neighbors.txt"
OUTPUT_FILE = "user_based_recommendations.csv"

TOP_N = 1000          # top-N items per user
NUM_CORES = 60        # processes for multiprocessing

global_user_ratings = None   # {uid: {mid: rating}} shared via fork
global_user_means = None     # {uid: mean_rating} shared via fork


def load_ratings(path):
    print(f"loading ratings from {path}...")
    t0 = time.time()
    user_ratings = defaultdict(dict)
    user_means = {}

    try:
        with open(path, "r") as f:
            reader = csv.reader(f)
            try:
                next(reader)  # skip header if present
            except StopIteration:
                pass
            for row in reader:
                if not row:
                    continue
                try:
                    uid, mid, r = row[0], row[1], float(row[2])  # userId, movieId, rating
                    user_ratings[uid][mid] = r
                except ValueError:
                    continue
    except FileNotFoundError:
        print(f"ratings file not found: {path}")
        exit(1)

    for uid, movies in user_ratings.items():
        if movies:
            user_means[uid] = sum(movies.values()) / len(movies)  # mean rating per user
        else:
            user_means[uid] = 3.0  # fallback mean if user has no ratings

    print(f"ratings loaded in {time.time() - t0:.2f}s")
    return user_ratings, user_means


def load_neighbors(path):
    print(f"loading neighbors from {path}...")
    tasks = []

    with open(path, "r") as f:
        for line in f:
            parts = line.strip().split("\t")
            if not parts:
                continue
            user_id = parts[0]
            neighbors_str = parts[1] if len(parts) > 1 else ""
            neighbors = []
            if neighbors_str:
                for item in neighbors_str.split(","):
                    if ":" in item:
                        nid, sim = item.split(":")
                        try:
                            neighbors.append((nid, float(sim)))  # neighbor id + similarity
                        except ValueError:
                            continue
            if neighbors:  # only keep users that have neighbors
                tasks.append((user_id, neighbors))

    print(f"neighbors loaded: {len(tasks)} users")
    return tasks


def process_user(task):
    target_user, neighbors = task
    user_ratings = global_user_ratings
    user_means = global_user_means

    if target_user not in user_ratings:
        return []

    target_mean = user_means[target_user]
    watched = set(user_ratings[target_user].keys())  # items already seen by target user

    candidate_scores = defaultdict(float)  # Σ sim(u,v)*(r(v,i)-μ_v)
    sim_sums = defaultdict(float)         # Σ |sim(u,v)|

    for neighbor_id, sim in neighbors:
        if neighbor_id not in user_ratings:
            continue
        neighbor_mean = user_means.get(neighbor_id, 3.0)

        for movie_id, rating in user_ratings[neighbor_id].items():
            if movie_id in watched:
                continue
            bias = rating - neighbor_mean
            candidate_scores[movie_id] += sim * bias  # accumulate mean-centered contribution
            sim_sums[movie_id] += abs(sim)            # L1 norm of similarities

    preds = []
    for movie_id, score_sum in candidate_scores.items():
        if sim_sums[movie_id] == 0:
            continue
        pred = target_mean + score_sum / sim_sums[movie_id]  # μ_u + Σ / Σ|sim|
        pred = max(0.5, min(5.0, pred))  # clamp to rating scale
        preds.append((movie_id, pred))

    preds.sort(key=lambda x: x[1], reverse=True)  # sort by predicted score desc
    return [(target_user, m, f"{s:.2f}") for m, s in preds[:TOP_N]]  # user, item, score


def init_worker(ratings, means):
    global global_user_ratings, global_user_means
    global_user_ratings = ratings
    global_user_means = means  # set shared references in each worker


if __name__ == "__main__":
    ratings_data, means_data = load_ratings(RATINGS_FILE)
    tasks = load_neighbors(USER_NEIGHBORS_FILE)

    print(f"start user-based recommendation ({NUM_CORES} cores)")
    t0 = time.time()
    results = []

    with Pool(
        processes=NUM_CORES,
        initializer=init_worker,
        initargs=(ratings_data, means_data),
    ) as pool:
        for res in pool.map(process_user, tasks, chunksize=50):  # chunksize to reduce overhead
            results.extend(res)

    print(f"prediction done in {time.time() - t0:.2f}s")
    print(f"writing output to {OUTPUT_FILE}...")

    with open(OUTPUT_FILE, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["userId", "movieId", "prediction"])  # header
        writer.writerows(results)

    print("done.")
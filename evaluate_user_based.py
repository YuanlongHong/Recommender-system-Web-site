import csv
import math
import time
from collections import defaultdict
from multiprocessing import Pool

# config
TRAIN_FILE = "ratings_train.csv"
TEST_FILE = "ratings_test.csv"
NEIGHBORS_FILE = "user_topk_neighbors.txt"
NUM_CORES = 60

# shared in workers
global_train_ratings = None   # {uid: {mid: rating}}
global_train_means = None     # {uid: mean_rating}
global_neighbors = None       # {uid: [(nid, sim), ...]}


def load_train_data(path):
    print("loading train...")
    t0 = time.time()
    user_ratings = defaultdict(dict)
    user_means = {}

    try:
        with open(path, "r") as f:
            reader = csv.reader(f)
            try:
                next(reader)  # skip header if any
            except StopIteration:
                pass
            for row in reader:
                if not row:
                    continue
                try:
                    uid, mid, rating = row[0], row[1], float(row[2])  # userId, movieId, rating
                    user_ratings[uid][mid] = rating
                except Exception:
                    continue
    except FileNotFoundError:
        print(f"train file not found: {path}")
        exit(1)

    for uid, movies in user_ratings.items():
        user_means[uid] = sum(movies.values()) / len(movies)  # mean rating per user

    print(f"train loaded in {time.time() - t0:.2f}s")
    return user_ratings, user_means


def load_neighbors(path):
    print("loading neighbors...")
    neighbors_map = {}

    try:
        with open(path, "r") as f:
            for line in f:
                parts = line.strip().split("\t")
                if not parts:
                    continue
                uid = parts[0]
                n_list = []
                if len(parts) > 1 and parts[1]:
                    for x in parts[1].split(","):
                        if ":" in x:
                            nid, sim = x.split(":")
                            try:
                                n_list.append((nid, float(sim)))  # neighbor id + similarity
                            except ValueError:
                                continue
                neighbors_map[uid] = n_list  # may be empty if user has no neighbors
    except FileNotFoundError:
        print(f"neighbor file not found: {path}")
        exit(1)

    print(f"neighbors loaded: {len(neighbors_map)} users")
    return neighbors_map


def load_test_data(path):
    print("loading test...")
    data = []

    try:
        with open(path, "r") as f:
            reader = csv.reader(f)
            try:
                next(reader)  # skip header if any
            except StopIteration:
                pass
            for row in f:
                row = row.strip().split(",")
                if not row:
                    continue
                try:
                    data.append((row[0], row[1], float(row[2])))  # (userId, movieId, rating)
                except Exception:
                    continue
    except FileNotFoundError:
        print(f"test file not found: {path}")
        exit(1)

    print(f"test loaded: {len(data)} rows")
    return data


def init_worker(train_ratings, train_means, neighbors):
    global global_train_ratings, global_train_means, global_neighbors
    global_train_ratings = train_ratings   # shared via fork
    global_train_means = train_means       # shared via fork
    global_neighbors = neighbors


def process_batch(batch):
    # compute error on a chunk of test triples
    user_ratings = global_train_ratings
    user_means = global_train_means
    neighbors_map = global_neighbors

    sse = 0.0  # sum of squared errors
    sae = 0.0  # sum of absolute errors
    cnt = 0    # number of predicted samples

    for user_id, movie_id, real_rating in batch:
        pred = None  # default: no prediction

        if user_id in user_means and user_id in neighbors_map and neighbors_map[user_id]:
            target_mean = user_means[user_id]  # μ_u
            score_sum = 0.0                   # Σ sim(u,v)*(r(v,i)-μ_v)
            sim_sum = 0.0                     # Σ |sim(u,v)|

            for neighbor_id, sim in neighbors_map[user_id]:
                if neighbor_id in user_ratings and movie_id in user_ratings[neighbor_id]:
                    r = user_ratings[neighbor_id][movie_id]
                    n_mean = user_means[neighbor_id]
                    score_sum += sim * (r - n_mean)  # mean-centered contribution
                    sim_sum += abs(sim)

            if sim_sum != 0.0:
                pred = target_mean + score_sum / sim_sum  # μ_u + Σ / Σ|sim|

        if pred is not None:
            pred = max(0.5, min(5.0, pred))  # clamp to rating scale
            err = pred - real_rating
            sse += err * err
            sae += abs(err)
            cnt += 1

    return sse, sae, cnt


if __name__ == "__main__":
    train_r, train_m = load_train_data(TRAIN_FILE)
    neighbors = load_neighbors(NEIGHBORS_FILE)
    test_list = load_test_data(TEST_FILE)

    print(f"start user-based eval ({NUM_CORES} cores)")
    t0 = time.time()

    chunk_size = 5000  # test batch size per task
    chunks = [test_list[i : i + chunk_size] for i in range(0, len(test_list), chunk_size)]

    total_sse = 0.0
    total_sae = 0.0
    total_cnt = 0

    with Pool(
        processes=NUM_CORES,
        initializer=init_worker,
        initargs=(train_r, train_m, neighbors),
    ) as pool:
        for sse, sae, cnt in pool.map(process_batch, chunks):
            total_sse += sse
            total_sae += sae
            total_cnt += cnt

    t1 = time.time()

    if total_cnt > 0:
        rmse = math.sqrt(total_sse / total_cnt)      # root mean squared error
        mae = total_sae / total_cnt                  # mean absolute error
        cov = total_cnt / len(test_list) * 100.0     # coverage on test set

        print("\n=== user-based results ===")
        print(f"RMSE      : {rmse:.5f}")
        print(f"MAE       : {mae:.5f}")
        print(f"coverage  : {cov:.2f}% ({total_cnt}/{len(test_list)})")
        print(f"time      : {t1 - t0:.2f}s")
    else:
        print("no predictable samples; check neighbors vs train data")
import csv
import math
import time
from collections import defaultdict
from multiprocessing import Pool

# config
TRAIN_FILE = "ratings_train.csv"
TEST_FILE = "ratings_test.csv"
ITEM_NEIGHBORS_FILE = "item_topk_neighbors.txt"
NUM_CORES = 60

# shared in workers
global_user_history = None   # {user_id: {movie_id: rating}}
global_item_sims = None      # {movie_id: [(neighbor_id, sim), ...]}


def load_train_data(path):
    print("loading train...")
    t0 = time.time()
    user_history = defaultdict(dict)

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
                    uid, mid, rating = row[0], row[1], float(row[2])  # userId, movieId, rating
                    user_history[uid][mid] = rating
                except Exception:
                    continue
    except FileNotFoundError:
        print(f"train file not found: {path}")
        exit(1)

    print(f"train loaded in {time.time() - t0:.2f}s")
    return user_history


def load_item_sims(path):
    print("loading item sims...")
    item_sims = {}

    try:
        with open(path, "r") as f:
            for line in f:
                parts = line.strip().split("\t")
                if not parts:
                    continue
                item_id = parts[0]              # target item id
                sims = []                      # list of (neighbor_id, sim)
                if len(parts) > 1 and parts[1]:
                    for x in parts[1].split(","):
                        if ":" in x:
                            nid, s = x.split(":")
                            try:
                                sims.append((nid, float(s)))
                            except ValueError:
                                continue
                item_sims[item_id] = sims
    except FileNotFoundError:
        print(f"item sim file not found: {path}")
        exit(1)

    print(f"item sims loaded: {len(item_sims)} items")
    return item_sims


def load_test_data(path):
    print("loading test...")
    data = []

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
                    data.append((row[0], row[1], float(row[2])))  # (userId, movieId, rating)
                except Exception:
                    continue
    except FileNotFoundError:
        print(f"test file not found: {path}")
        exit(1)

    print(f"test loaded: {len(data)} rows")
    return data


def init_worker(user_hist, item_sims):
    global global_user_history, global_item_sims
    global_user_history = user_hist    # shared user history via fork
    global_item_sims = item_sims      # shared item similarity table


def process_batch(batch):
    # item-based CF on a batch of (u, i, r)
    user_history = global_user_history
    item_sims = global_item_sims

    sse = 0.0  # sum of squared errors
    sae = 0.0  # sum of absolute errors
    cnt = 0    # number of predicted samples

    for user_id, target_movie, real_rating in batch:
        pred = None

        if user_id in user_history and target_movie in item_sims:
            weighted = 0.0  # Σ sim(i,j)*r(u,j)
            sim_sum = 0.0   # Σ |sim(i,j)|

            for sim_movie, sim_score in item_sims[target_movie]:
                if sim_movie in user_history[user_id]:
                    r = user_history[user_id][sim_movie]
                    weighted += sim_score * r       # accumulate weighted rating
                    sim_sum += abs(sim_score)       # accumulate similarity weight

            if sim_sum > 0:
                pred = weighted / sim_sum           # weighted average over similar items

        if pred is not None:
            pred = max(0.5, min(5.0, pred))         # clamp to rating scale
            err = pred - real_rating
            sse += err * err
            sae += abs(err)
            cnt += 1

    return sse, sae, cnt


if __name__ == "__main__":
    # load data
    u_hist = load_train_data(TRAIN_FILE)
    item_sims = load_item_sims(ITEM_NEIGHBORS_FILE)
    test_list = load_test_data(TEST_FILE)

    print(f"start item-based eval ({NUM_CORES} cores)")
    t0 = time.time()

    chunk_size = 5000  # test batch size per task
    chunks = [test_list[i : i + chunk_size] for i in range(0, len(test_list), chunk_size)]

    total_sse = 0.0
    total_sae = 0.0
    total_cnt = 0

    with Pool(
        processes=NUM_CORES,
        initializer=init_worker,
        initargs=(u_hist, item_sims),
    ) as pool:
        for sse, sae, cnt in pool.map(process_batch, chunks):
            total_sse += sse
            total_sae += sae
            total_cnt += cnt

    t1 = time.time()

    if total_cnt > 0:
        rmse = math.sqrt(total_sse / total_cnt)          # root mean squared error
        mae = total_sae / total_cnt                      # mean absolute error
        cov = total_cnt / len(test_list) * 100.0         # coverage on test set

        print("\nitem-based results:")
        print(f"RMSE      : {rmse:.5f}")
        print(f"MAE       : {mae:.5f}")
        print(f"coverage  : {cov:.2f}% ({total_cnt}/{len(test_list)})")
        print(f"time      : {t1 - t0:.2f}s")
    else:
        print("no predictable samples; check item IDs vs similarity file")
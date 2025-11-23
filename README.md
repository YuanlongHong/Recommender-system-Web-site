# MovieLens 32M Recommender System Web Demo

This repository implements a **large-scale movie recommender system** on the MovieLens 32M ratings:

- **User-based CF** and **Item-based CF**
- **Hadoop / AWS EMR** for similarity computation on 30M+ ratings
- **Offline evaluation** (RMSE / MAE / coverage)
- **Flask web UI + MySQL** to browse user history, top-K recommendations and similar users
- **TMDB API** to enrich movies with posters and overviews

All large intermediate / final data files are hosted in a public S3 bucket:

    s3://draco-movielens32m-recsys/
        ├── raw-data/                         # (optional) original MovieLens files
        ├── train-test-data/
        │   ├── ratings_train.csv             # ~400MB
        │   └── ratings_test.csv              # ~100MB
        ├── Hadoop-result-50neighbors/
        │   ├── user_topk_neighbors.txt       # ~71MB
        │   └── item_topk_neighbors.txt       # ~40MB
        └── predict-result/
            └── user_based_recommendations.csv    # ~1.9GB (final predictions)

---

## 1. Project structure

    Recommender-system-Web-site/
    ├── Hadoop/                  # Hadoop Streaming mappers/reducers, EMR job scripts
    ├── data-preprocessing/      # Local preprocessing: train/test split, etc.
    ├── eval/                    # Offline evaluation & bulk prediction (user/item-based)
    ├── Web/                     # Flask + MySQL web application
    └── terminalcode_emr/        # Useful EMR / Hadoop CLI commands

### 1.1 `data-preprocessing/`

Small local scripts to prepare MovieLens ratings for Hadoop.

- `split_train_test.py`
  - Reads `ratings.csv` (MovieLens full ratings)
  - Splits into `ratings_train.csv` and `ratings_test.csv` with an 80/20 random split
  - Outputs **header-less** CSVs (better for Hadoop Streaming)

Generated files are uploaded to:

- `s3://draco-movielens32m-recsys/train-test-data/`

---

### 1.2 `Hadoop/`

MapReduce pipeline for collaborative filtering on **AWS EMR**.

High-level stages:

1. **MR1–MR2** – Build user–user (or item–item) co-rating statistics  
   - Input: `ratings_train.csv` on HDFS  
   - Output: pairs of users/items with the list of co-rated movies

2. **MR3** – Compute cosine similarity  
   - For each user–user or item–item pair, compute cosine similarity over their rating vectors  
   - Apply thresholds on the number of co-rated items to control noise and data explosion

3. **MR4** – Select top-K neighbors  
   - For each user (or item), keep only the top-K neighbors with highest similarity  
   - K = 50 in this project

Final outputs:

- `user_topk_neighbors.txt`
- `item_topk_neighbors.txt`

These are stored in:

- `s3://draco-movielens32m-recsys/Hadoop-result-50neighbors/`

---

### 1.3 `eval/` (offline evaluation & prediction)

Python scripts that run **offline on a single machine**, using the pre-computed Hadoop results.

**Common inputs**

- `train-test-data/ratings_train.csv`
- `train-test-data/ratings_test.csv`
- `Hadoop-result-50neighbors/user_topk_neighbors.txt`
- `Hadoop-result-50neighbors/item_topk_neighbors.txt`

Scripts:

#### `evaluate_user_based.py`

- Loads
  - training ratings → `user_ratings[uid][mid] = rating`
  - per-user mean rating → `user_means[uid]`
  - `user_topk_neighbors.txt` → neighbors per user
- For each `(user, movie, rating)` in test set:
  - Check if the user has neighbors who rated this movie
  - Use **mean-centered weighted sum**:

        pred(u, i) = μ_u + ( Σ_v sim(u,v) · (r_v,i − μ_v) ) / Σ_v |sim(u,v)|

- Uses `multiprocessing.Pool` with a fixed chunk size
- Reports:
  - RMSE
  - MAE
  - Coverage = (#predicted samples) / (#all test samples)
  - Runtime

#### `evaluate_item_based.py`

- Similar structure, but **item-based**:
  - Uses `item_topk_neighbors.txt`
  - For each `(user, target_movie, rating)`:
    - Look at similar movies rated by the user
    - Predict by similarity-weighted average:

          pred(u, i) = Σ_j sim(i,j) · r_u,j / Σ_j |sim(i,j)|

- Same metrics: RMSE / MAE / coverage / time

#### `user_based_predict.py` (naming may vary)

- Uses:
  - `ratings_train.csv`
  - `user_topk_neighbors.txt`
- For each user:
  - Collect all movies rated by neighbors but **not** by the user
  - Predict scores for all such (user, movie) pairs
  - Write one big CSV:

        userId,movieId,prediction
        1,318,4.72
        1,858,4.65
        ...

- Result file:

  - `user_based_recommendations.csv` (~1.9GB)
  - Stored at `s3://draco-movielens32m-recsys/predict-result/user_based_recommendations.csv`

You can run these scripts locally:

    cd eval

    # user-based evaluation
    python evaluate_user_based.py

    # item-based evaluation
    python evaluate_item_based.py

    # full prediction (user-based)
    python user_based_predict.py

---

### 1.4 `Web/` (Flask web app)

Flask application to **browse** the recommendations and rating history stored in MySQL.

Key pieces:

#### `data_loader.py`

Reads all necessary data from MySQL:

- `movies` table: `movieId, title, genres`
- `ratings_train` table: historical ratings
- `user_topk_neighbors` table: neighbor list per user (stored as `"uid1:sim1,uid2:sim2,...“`)
- `user_item_predictions` table: imported from `user_based_recommendations.csv`
- Produces:

      {
          "movies":        {movieId: {"title": str, "genres": str}},
          "user_ratings":  {userId: [(movieId, rating), ...]},
          "user_neighbors":{userId: [(neighborId, sim), ...]},
          "user_recs":     {userId: [(movieId, prediction), ...]},
          "all_users":     [userId1, userId2, ...],
      }

#### `app.py`

Main Flask app:

- `/login`  
  - Simple login page  
  - User can input a `userId` or select from dropdown (`all_users`)  
  - Accepts if the user appears in ratings or recommendations

- `/dashboard`  
  - Requires `user_id` in session  
  - For this user, it builds:
    - **Rated movies**: list of (movie, rating) from `ratings_train`
    - **Recommended movies**: top-30 predicted movies from `user_item_predictions`
    - **Similar users**: neighbors from `user_topk_neighbors`
  - Calls TMDB helpers to ensure poster / overview are present for all movies
  - Renders `templates/dashboard.html`

#### `Crawler/` (TMDB helpers)

- `tmdb_service.py`
  - `ensure_tmdb_for_movie_ids(movie_ids)`  
    - Checks which of these IDs have metadata in table `movies_tmdb`
    - For missing ones, looks up TMDB using `links` table and API, then upserts rows
  - `load_tmdb_map(movie_ids)`  
    - Returns `movie_id -> {poster_path, overview, title_tmdb}` for use in templates
  - `get_poster_base_url()`  
    - Small helper to build full image URLs

- `tmdb_utils.py`
  - `get_mysql_connection()` and `ensure_movies_tmdb_table()` for table `movies_tmdb`
  - `build_imdb_id(raw_imdb_id)` to turn MovieLens numeric IDs into `ttxxxxxxx`
  - `fetch_tmdb_by_id(id)` / `fetch_tmdb_by_imdb(imdb_id)` to call TMDB API
  - `upsert_movie(...)` to insert/update `movies_tmdb` rows

- `config.py`
  - Config for:
    - `TMDB_API_KEY`
    - MySQL connection
    - `POSTER_BASE_URL`
  - **Important:** in a public repo, use placeholders or environment variables instead of real credentials.

#### `templates/`

- `dashboard.html`
  - Main UI with three sections:
    - **Movies you have rated**
    - **Recommended movies for you**
    - **Users similar to you**
  - For each movie:
    - Displays title, genres, rating / prediction, link to MovieLens
    - If TMDB metadata exists:
      - Shows a small poster thumbnail
      - On hover, a dark overlay pops up with the movie overview

- `login.html`
  - Simple login form with:
    - text input for `user_id`
    - dropdown of example users (optional)
    - error messages if the user ID is invalid

To run the web app locally:

    cd Web
    # make sure Python deps and MySQL config are set
    python app.py

Then open:

- http://127.0.0.1:5000

---

### 1.5 `terminalcode_emr/`

Helper notes and snippets used during development on AWS EMR, for example:

- `aws s3 cp` commands to upload / download code and results
- `hadoop jar ...` commands for Hadoop Streaming
- cluster setup notes (EMR version, instance types, etc.)

These are mainly for reproducibility and debugging rather than core logic.

---

## 2. Datasets & S3 layout

The project uses the **MovieLens 32M** rating dataset. To reduce EMR cost for others, all heavy intermediate results are pre-computed and uploaded to a public S3 bucket:

    s3://draco-movielens32m-recsys/
        ├── raw-data/                         # (optional) original MovieLens files
        ├── train-test-data/
        │   ├── ratings_train.csv
        │   └── ratings_test.csv
        ├── Hadoop-result-50neighbors/
        │   ├── user_topk_neighbors.txt
        │   └── item_topk_neighbors.txt
        └── predict-result/
            └── user_based_recommendations.csv

You can either:

- Download these files with `aws s3 cp`, or  
- Reproduce them by re-running the Hadoop pipeline on your own EMR cluster.

---

## 3. Environment setup

### 3.1 Python dependencies

Recommended:

- Python 3.9+
- `flask`
- `pymysql`
- `requests`
- `numpy`
- `pandas`

Example:

    python -m venv venv
    source venv/bin/activate      # Windows: .\venv\Scripts\activate
    pip install -r requirements.txt    # if you add one

### 3.2 MySQL

1. Create a database, e.g. `cse482`.
2. Import MovieLens tables:
   - `movies`
   - `links`
   - `ratings_train`
   - `ratings_test`
3. Import Hadoop / eval results:
   - `user_topk_neighbors`      (parsed from `user_topk_neighbors.txt`)
   - `user_item_predictions`    (from `user_based_recommendations.csv`)
4. TMDB metadata:
   - Table `movies_tmdb` will be created automatically by the crawler if missing.

Update configs:

- `Web/data_loader.py` → `DB_CONFIG`
- `Web/Crawler/config.py` → `MYSQL_*` and `TMDB_API_KEY`

**Do not commit real passwords or API keys to the public repo.**

---

## 4. Running summary

- **Preprocessing**  
  - `data-preprocessing/split_train_test.py` → create train/test splits

- **Hadoop (on EMR)**  
  - Run MR1–MR4 jobs in `Hadoop/` to compute neighbors  
  - Collect `user_topk_neighbors.txt`, `item_topk_neighbors.txt`

- **Offline evaluation & prediction**  
  - `eval/evaluate_user_based.py`  
  - `eval/evaluate_item_based.py`  
  - `eval/user_based_predict.py` → `user_based_recommendations.csv`

- **Web demo**  
  - Import predictions into MySQL (`user_item_predictions`)  
  - `cd Web && python app.py`  
  - Visit `http://127.0.0.1:5000`

---

## 5. Notes

- This repo is mainly for teaching / course project purposes (Big Data & Recommender Systems).
- The S3 bucket `draco-movielens32m-recsys` is used to share large files so others can reproduce results without re-running Hadoop.
- When forking or reusing this project:
  - Replace S3 bucket names, credentials, and API keys with your own.
  - Consider using environment variables (`.env`) or a config file that is **not** committed to version control.

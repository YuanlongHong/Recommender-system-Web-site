# MovieLens 32M Recommender System Web Demo

This repository implements a **large-scale movie recommender system** on the MovieLens 32M ratings:

* **User-based CF** and **Item-based CF**
* **Hadoop / AWS EMR** for similarity computation on 30M+ ratings
* **Offline evaluation** (RMSE / MAE / coverage)
* **Flask web UI + MySQL** to browse user history, top-K recommendations and similar users
* **TMDB API** to enrich movies with posters and overviews

All large intermediate / final data files are hosted in a public S3 bucket:

```bash
s3://draco-movielens32m-recsys/
    ├── raw-data/                     # (optional) original MovieLens files
    ├── train-test-data/
    │   ├── ratings_train.csv         # ~400MB
    │   └── ratings_test.csv          # ~100MB
    ├── Hadoop-result-50neighbors/
    │   ├── user_topk_neighbors.txt   # ~71MB
    │   └── item_topk_neighbors.txt   # ~40MB
    └── predict-result/
        └── user_based_recommendations.csv   # ~1.9GB (final predictions)

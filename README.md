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

1. Project structure

Recommender-system-Web-site/
├── Hadoop/                  # Hadoop Streaming mappers/reducers, EMR job scripts
├── data-preprocessing/      # Local preprocessing: train/test split, etc.
├── eval/                    # Offline evaluation & bulk prediction (user/item-based)
├── Web/                     # Flask + MySQL web application
└── terminalcode_emr/        # Useful EMR / Hadoop CLI commands


1.1 data-preprocessing/

Small local scripts to prepare MovieLens ratings for Hadoop:
	•	split_train_test.py
	•	Reads ratings.csv (MovieLens full ratings)
	•	Splits into ratings_train.csv and ratings_test.csv by random 80/20
	•	Outputs header-less CSVs (better for Hadoop Streaming)

1.2 Hadoop/

MapReduce pipeline for collaborative filtering on AWS EMR:
	•	Stage MR1–MR2: build user–user co-rating statistics
	•	Stage MR3: compute cosine similarity between users / items
	•	Stage MR4: generate top-K neighbors per user / per item
	•	Outputs:
	•	user_topk_neighbors.txt
	•	item_topk_neighbors.txt

The final outputs are uploaded to:
s3://draco-movielens32m-recsys/Hadoop-result-50neighbors/

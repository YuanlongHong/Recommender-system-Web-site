import pandas as pd

# I/O config
INPUT_FILE = "ratings.csv"
TRAIN_OUTPUT = "ratings_train.csv"
TEST_OUTPUT = "ratings_test.csv"
SPLIT_RATIO = 0.8  # 80/20 split


def split_dataset():
    """Create train/test split from ratings data."""
    try:
        df = pd.read_csv(INPUT_FILE, usecols=[0, 1, 2], engine="c")
    except ValueError:
        # no header case
        df = pd.read_csv(
            INPUT_FILE,
            header=None,
            names=["userId", "movieId", "rating"],
            usecols=[0, 1, 2],
            engine="c",
        )

    # reproducible random split
    train = df.sample(frac=SPLIT_RATIO, random_state=42)
    test = df.drop(train.index)

    # Hadoop-friendly: no index, no header
    train.to_csv(TRAIN_OUTPUT, index=False, header=False)
    test.to_csv(TEST_OUTPUT, index=False, header=False)


if __name__ == "__main__":
    split_dataset()
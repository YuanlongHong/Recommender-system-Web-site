# Crawler/tmdb_utils.py

from typing import Optional, Dict, Any

import requests
import pymysql

from .config import TMDB_API_KEY, MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB


#DB helpers

def get_mysql_connection():
    """Create a new MySQL connection."""
    conn = pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB,
        charset="utf8mb4",
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor,
    )
    return conn


def ensure_movies_tmdb_table(conn):
    """Create movies_tmdb table if it does not exist."""
    create_sql = """
    CREATE TABLE IF NOT EXISTS movies_tmdb (
        movie_id INT PRIMARY KEY,
        tmdb_id INT,
        imdb_id VARCHAR(16),
        title_tmdb VARCHAR(255),
        overview TEXT,
        release_date DATE,
        runtime INT,
        vote_average FLOAT,
        vote_count INT,
        popularity FLOAT,
        original_language VARCHAR(8),
        genres_tmdb VARCHAR(255),
        poster_path VARCHAR(255),
        backdrop_path VARCHAR(255),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
    """
    with conn.cursor() as cur:
        cur.execute(create_sql)


#TMDB helpers

def build_imdb_id(raw_imdb_id: Optional[str]) -> Optional[str]:
    """
    Convert numeric imdbId from MovieLens (e.g. 114709)
    to standard form 'tt0114709'.
    """
    if raw_imdb_id is None:
        return None
    s = str(raw_imdb_id).strip()
    if not s.isdigit():
        return None
    return f"tt{int(s):07d}"  # zero-pad to 7 digits


def fetch_tmdb_by_id(tmdb_id: str) -> Optional[Dict[str, Any]]:
    """Fetch TMDB movie detail by tmdbId."""
    if not tmdb_id:
        return None

    url = f"https://api.themoviedb.org/3/movie/{tmdb_id}"
    params = {"api_key": TMDB_API_KEY, "language": "en-US"}
    try:
        resp = requests.get(url, params=params, timeout=10)
        if resp.status_code == 200:
            return resp.json()
        else:
            print(f"[WARN] TMDB id {tmdb_id} request failed, status={resp.status_code}")
            return None
    except Exception as e:
        print(f"[ERROR] TMDB request by id={tmdb_id} failed: {e}")
        return None


def fetch_tmdb_by_imdb(imdb_id: str) -> Optional[Dict[str, Any]]:
    """Fetch TMDB movie detail by IMDB id."""
    if not imdb_id:
        return None

    url = f"https://api.themoviedb.org/3/find/{imdb_id}"
    params = {"api_key": TMDB_API_KEY, "external_source": "imdb_id"}
    try:
        resp = requests.get(url, params=params, timeout=10)
        if resp.status_code != 200:
            print(f"[WARN] TMDB find for IMDb {imdb_id} failed, status={resp.status_code}")
            return None

        data = resp.json()
        results = data.get("movie_results") or []  # TMDB returns list under movie_results
        if not results:
            print(f"[INFO] No movie_results for IMDb {imdb_id}")
            return None

        tmdb_id = results[0].get("id")  # take first match
        if not tmdb_id:
            return None

        return fetch_tmdb_by_id(str(tmdb_id))
    except Exception as e:
        print(f"[ERROR] TMDB lookup by IMDb={imdb_id} failed: {e}")
        return None


def parse_genres(genres_list) -> str:
    """Convert TMDB genres list to 'Drama,Comedy' string."""
    if not genres_list:
        return ""
    names = [g.get("name", "").strip() for g in genres_list if g.get("name")]
    return ",".join(names)


def upsert_movie(conn, movie_id: int, imdb_id: Optional[str], tmdb_json: Dict[str, Any]):
    """Insert or update a row in movies_tmdb based on TMDB JSON."""
    if not tmdb_json:
        return

    tmdb_id = tmdb_json.get("id")
    title_tmdb = tmdb_json.get("title")
    overview = tmdb_json.get("overview")
    release_date = tmdb_json.get("release_date") or None  # may be empty string
    runtime = tmdb_json.get("runtime")
    vote_average = tmdb_json.get("vote_average")
    vote_count = tmdb_json.get("vote_count")
    popularity = tmdb_json.get("popularity")
    original_language = tmdb_json.get("original_language")
    genres_tmdb = parse_genres(tmdb_json.get("genres"))  # normalize to comma-separated names
    poster_path = tmdb_json.get("poster_path")
    backdrop_path = tmdb_json.get("backdrop_path")

    sql = """
    INSERT INTO movies_tmdb (
        movie_id, tmdb_id, imdb_id, title_tmdb, overview,
        release_date, runtime, vote_average, vote_count,
        popularity, original_language, genres_tmdb,
        poster_path, backdrop_path
    ) VALUES (
        %(movie_id)s, %(tmdb_id)s, %(imdb_id)s, %(title_tmdb)s, %(overview)s,
        %(release_date)s, %(runtime)s, %(vote_average)s, %(vote_count)s,
        %(popularity)s, %(original_language)s, %(genres_tmdb)s,
        %(poster_path)s, %(backdrop_path)s
    )
    ON DUPLICATE KEY UPDATE
        tmdb_id = VALUES(tmdb_id),
        imdb_id = VALUES(imdb_id),
        title_tmdb = VALUES(title_tmdb),
        overview = VALUES(overview),
        release_date = VALUES(release_date),
        runtime = VALUES(runtime),
        vote_average = VALUES(vote_average),
        vote_count = VALUES(vote_count),
        popularity = VALUES(popularity),
        original_language = VALUES(original_language),
        genres_tmdb = VALUES(genres_tmdb),
        poster_path = VALUES(poster_path),
        backdrop_path = VALUES(backdrop_path);
    """

    params = {
        "movie_id": movie_id,
        "tmdb_id": tmdb_id,
        "imdb_id": imdb_id,
        "title_tmdb": title_tmdb,
        "overview": overview,
        "release_date": release_date,
        "runtime": runtime,
        "vote_average": vote_average,
        "vote_count": vote_count,
        "popularity": popularity,
        "original_language": original_language,
        "genres_tmdb": genres_tmdb,
        "poster_path": poster_path,
        "backdrop_path": backdrop_path,
    }

    with conn.cursor() as cur:
        cur.execute(sql, params)  # INSERT ... ON DUPLICATE KEY UPDATE
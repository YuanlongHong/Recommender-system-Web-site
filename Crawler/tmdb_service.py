# Crawler/tmdb_service.py

from typing import Iterable, Dict, Any, Set

from .tmdb_utils import (
    get_mysql_connection,
    ensure_movies_tmdb_table,
    build_imdb_id,
    fetch_tmdb_by_id,
    fetch_tmdb_by_imdb,
    upsert_movie,
)
from .config import POSTER_BASE_URL


def ensure_tmdb_for_movie(conn, movie_id: int):
    """Fill TMDB metadata for a single movie (using links table)."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT imdbId, tmdbId FROM links WHERE movieId = %s",
            (movie_id,),
        )
        row = cur.fetchone()

    if not row:
        return

    imdb_raw = row["imdbId"]
    tmdb_raw = row["tmdbId"]

    imdb_id = build_imdb_id(imdb_raw)  # zero-pad + prefix to standard IMDB id

    tmdb_json = None
    if tmdb_raw is not None and str(tmdb_raw).strip().isdigit():
        tmdb_json = fetch_tmdb_by_id(str(tmdb_raw).strip())  # lookup by TMDB id
    elif imdb_id:
        tmdb_json = fetch_tmdb_by_imdb(imdb_id)              # fallback: lookup by IMDB id

    if tmdb_json:
        upsert_movie(conn, movie_id, imdb_id, tmdb_json)     # insert or update movies_tmdb row


def ensure_tmdb_for_movie_ids(movie_ids: Iterable[int]):
    """
    Ensure TMDB info for a batch of movieIds:
    only fetch for those missing in movies_tmdb.
    """
    ids: Set[int] = {int(mid) for mid in movie_ids}  # normalize to int + unique
    if not ids:
        return

    conn = get_mysql_connection()
    ensure_movies_tmdb_table(conn)  # create movies_tmdb if not exists

    ids_tuple = tuple(ids)
    placeholder = ",".join(["%s"] * len(ids_tuple))  # dynamic IN (...) placeholder

    # 1) find movies that already have TMDB metadata
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT movie_id FROM movies_tmdb WHERE movie_id IN ({placeholder})",
            ids_tuple,
        )
        have_meta = {row["movie_id"] for row in cur}

    missing = ids - have_meta  # only fetch for missing ones
    print(f"[TMDB] need to fill {len(missing)} movies")

    # 2) fetch TMDB only for missing movies
    for mid in missing:
        ensure_tmdb_for_movie(conn, mid)

    conn.close()


def load_tmdb_map(movie_ids: Iterable[int]) -> Dict[int, Dict[str, Any]]:
    """
    Load TMDB fields for given movieIds from movies_tmdb.
    Returns: {movie_id: {poster_path, overview, title_tmdb}, ...}
    """
    ids: Set[int] = {int(mid) for mid in movie_ids}
    if not ids:
        return {}

    conn = get_mysql_connection()
    ids_tuple = tuple(ids)
    placeholder = ",".join(["%s"] * len(ids_tuple))

    sql = f"""
    SELECT movie_id, poster_path, overview, title_tmdb
    FROM movies_tmdb
    WHERE movie_id IN ({placeholder})
    """
    with conn.cursor() as cur:
        cur.execute(sql, ids_tuple)
        rows = cur.fetchall()
    conn.close()

    result: Dict[int, Dict[str, Any]] = {}
    for r in rows:
        result[r["movie_id"]] = {
            "poster_path": r["poster_path"],
            "overview": r["overview"],
            "title_tmdb": r["title_tmdb"],
        }
    return result


def get_poster_base_url() -> str:
    """Base URL prefix for TMDB poster images."""
    return POSTER_BASE_URL  # e.g. https://image.tmdb.org/t/p/w342/
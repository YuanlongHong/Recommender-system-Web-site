#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
从 MySQL 中的 links 表读取 movieId / imdbId / tmdbId，
调用 TMDB API 获取电影详情，并写入 MySQL 表 movies_tmdb。

使用方法：
1. 先在 MySQL 里建好 movielens 库，并导入 MovieLens 的 links.csv 为 links 表
   表结构含有至少三列：movieId, imdbId, tmdbId
2. 修改下面的 TMDB_API_KEY 和 MySQL 配置
3. 安装依赖：
   pip install requests pymysql
4. 运行：
   python3 fetch_tmdb_from_links_table.py
"""

import time
from typing import Optional, Dict, Any

import requests
import pymysql

# =============== 配置区域（需要你自己改） ===============

# 1. TMDB API Key（v3，那串比较短的）
TMDB_API_KEY = "fa9444c0c334500ea983d9e37bfd76cd"

# 2. MySQL 连接配置
MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_USER = "root"
MYSQL_PASSWORD = "@Hong13086728388"
MYSQL_DB = "movielens"     # 你的数据库名
LINKS_TABLE = "links"      # 存 links.csv 的表名

# 3. 其他配置
REQUEST_INTERVAL = 0.1    # 每次请求之间 sleep 秒数，防止触发频率限制

# =======================================================


def get_mysql_connection():
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


def ensure_table_exists(conn):
    """
    创建存 TMDB 信息的表（如果不存在）
    """
    create_sql = """
    CREATE TABLE IF NOT EXISTS movies_tmdb (
        movie_id INT PRIMARY KEY,
        tmdb_id INT,
        imdb_id VARCHAR(16),
        title VARCHAR(255),
        original_title VARCHAR(255),
        overview TEXT,
        release_date DATE,
        runtime INT,
        vote_average FLOAT,
        vote_count INT,
        popularity FLOAT,
        original_language VARCHAR(8),
        genres VARCHAR(255),
        poster_path VARCHAR(255),
        backdrop_path VARCHAR(255),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        INDEX idx_tmdb_id (tmdb_id)
    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
    """
    with conn.cursor() as cur:
        cur.execute(create_sql)


def build_imdb_id(raw_imdb_id: Optional[str]) -> Optional[str]:
    """
    links 表里的 imdbId 是数字，例如 114709，
    需要转成类似 'tt0114709' 这种格式。
    """
    if not raw_imdb_id:
        return None
    raw_imdb_id = str(raw_imdb_id).strip()
    if not raw_imdb_id.isdigit():
        return None
    return f"tt{int(raw_imdb_id):07d}"


def fetch_tmdb_by_id(tmdb_id: str) -> Optional[Dict[str, Any]]:
    """
    用 TMDB 电影 id 调 movie detail 接口：
    https://api.themoviedb.org/3/movie/{movie_id}
    """
    if not tmdb_id:
        return None

    url = f"https://api.themoviedb.org/3/movie/{tmdb_id}"
    params = {
        "api_key": TMDB_API_KEY,
        "language": "en-US",
    }
    try:
        resp = requests.get(url, params=params, timeout=10)
        if resp.status_code == 200:
            return resp.json()
        else:
            print(f"[WARN] TMDB id {tmdb_id} 请求失败，status={resp.status_code}")
            return None
    except Exception as e:
        print(f"[ERROR] 请求 TMDB id={tmdb_id} 出错: {e}")
        return None


def fetch_tmdb_by_imdb(imdb_id: str) -> Optional[Dict[str, Any]]:
    """
    如果 links 里 tmdbId 为空，就用 imdbId 去 TMDB 查找：
    https://api.themoviedb.org/3/find/{imdb_id}?external_source=imdb_id
    """
    if not imdb_id:
        return None

    url = f"https://api.themoviedb.org/3/find/{imdb_id}"
    params = {
        "api_key": TMDB_API_KEY,
        "external_source": "imdb_id",
    }
    try:
        resp = requests.get(url, params=params, timeout=10)
        if resp.status_code != 200:
            print(f"[WARN] 通过 IMDb {imdb_id} 查找 TMDB 失败，status={resp.status_code}")
            return None

        data = resp.json()
        results = data.get("movie_results") or []
        if not results:
            print(f"[INFO] IMDb {imdb_id} 在 TMDB 没有找到电影结果")
            return None

        # 默认取第一个结果
        movie_basic = results[0]
        tmdb_id = movie_basic.get("id")
        if not tmdb_id:
            return None

        # 再用 tmdb_id 拉一次详情，拿到完整字段
        return fetch_tmdb_by_id(str(tmdb_id))

    except Exception as e:
        print(f"[ERROR] 通过 IMDb={imdb_id} 查 TMDB 出错: {e}")
        return None


def parse_genres(genres_list) -> str:
    """
    TMDB 返回的 genres 是一个 dict 列表：[{id: xx, name: "Drama"}, ...]
    我们只存名称，用逗号拼起来。
    """
    if not genres_list:
        return ""
    names = [g.get("name", "").strip() for g in genres_list if g.get("name")]
    return ",".join(names)


def upsert_movie(conn, movie_id: int, imdb_id: Optional[str], tmdb_json: Dict[str, Any]):
    """
    把 TMDB 的 json 数据写到 movies_tmdb 表里。
    使用 ON DUPLICATE KEY UPDATE，所以可以重复跑脚本。
    """
    if not tmdb_json:
        return

    tmdb_id = tmdb_json.get("id")
    title = tmdb_json.get("title")
    original_title = tmdb_json.get("original_title")
    overview = tmdb_json.get("overview")
    release_date = tmdb_json.get("release_date") or None
    runtime = tmdb_json.get("runtime")
    vote_average = tmdb_json.get("vote_average")
    vote_count = tmdb_json.get("vote_count")
    popularity = tmdb_json.get("popularity")
    original_language = tmdb_json.get("original_language")
    genres = parse_genres(tmdb_json.get("genres"))
    poster_path = tmdb_json.get("poster_path")
    backdrop_path = tmdb_json.get("backdrop_path")

    sql = """
    INSERT INTO movies_tmdb (
        movie_id, tmdb_id, imdb_id, title, original_title, overview,
        release_date, runtime, vote_average, vote_count, popularity,
        original_language, genres, poster_path, backdrop_path
    ) VALUES (
        %(movie_id)s, %(tmdb_id)s, %(imdb_id)s, %(title)s, %(original_title)s, %(overview)s,
        %(release_date)s, %(runtime)s, %(vote_average)s, %(vote_count)s, %(popularity)s,
        %(original_language)s, %(genres)s, %(poster_path)s, %(backdrop_path)s
    )
    ON DUPLICATE KEY UPDATE
        tmdb_id = VALUES(tmdb_id),
        imdb_id = VALUES(imdb_id),
        title = VALUES(title),
        original_title = VALUES(original_title),
        overview = VALUES(overview),
        release_date = VALUES(release_date),
        runtime = VALUES(runtime),
        vote_average = VALUES(vote_average),
        vote_count = VALUES(vote_count),
        popularity = VALUES(popularity),
        original_language = VALUES(original_language),
        genres = VALUES(genres),
        poster_path = VALUES(poster_path),
        backdrop_path = VALUES(backdrop_path);
    """

    params = {
        "movie_id": movie_id,
        "tmdb_id": tmdb_id,
        "imdb_id": imdb_id,
        "title": title,
        "original_title": original_title,
        "overview": overview,
        "release_date": release_date,
        "runtime": runtime,
        "vote_average": vote_average,
        "vote_count": vote_count,
        "popularity": popularity,
        "original_language": original_language,
        "genres": genres,
        "poster_path": poster_path,
        "backdrop_path": backdrop_path,
    }

    with conn.cursor() as cur:
        cur.execute(sql, params)


def main():
    if TMDB_API_KEY == "YOUR_TMDB_V3_API_KEY_HERE":
        print("[ERROR] 请先在脚本顶部填好 TMDB_API_KEY 再运行。")
        return

    print("[INFO] 连接 MySQL...")
    conn = get_mysql_connection()
    ensure_table_exists(conn)
    print("[INFO] movies_tmdb 表已准备好。")

    total = 0
    success = 0
    skipped = 0

    with conn.cursor() as cur:
        # 这里直接从 links 表读出 movieId / imdbId / tmdbId
        sql = f"SELECT movieId, imdbId, tmdbId FROM {LINKS_TABLE} ORDER BY movieId"
        cur.execute(sql)

        for row in cur:
            total += 1
            movie_id = row["movieId"]
            imdb_raw = row["imdbId"]
            tmdb_raw = row["tmdbId"]

            imdb_id = build_imdb_id(imdb_raw)

            tmdb_json = None

            # ✅ 优先根据 links 表里的 tmdbId 查询
            if tmdb_raw is not None and str(tmdb_raw).strip().isdigit():
                tmdb_json = fetch_tmdb_by_id(str(tmdb_raw).strip())
            # 如果 tmdbId 空，再考虑用 imdbId 去 find
            elif imdb_id:
                tmdb_json = fetch_tmdb_by_imdb(imdb_id)

            if not tmdb_json:
                print(f"[INFO] movie_id={movie_id} 在 TMDB 找不到，跳过。")
                skipped += 1
            else:
                upsert_movie(conn, movie_id, imdb_id, tmdb_json)
                success += 1
                print(f"[OK] 已写入 movie_id={movie_id} (TMDB id={tmdb_json.get('id')})")

            time.sleep(REQUEST_INTERVAL)

    conn.close()
    print("\n========== 任务完成 ==========")
    print(f"总行数: {total}")
    print(f"成功写入: {success}")
    print(f"未找到/跳过: {skipped}")


if __name__ == "__main__":
    main()
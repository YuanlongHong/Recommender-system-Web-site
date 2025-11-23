# app.py
from flask import Flask, render_template, request, redirect, session
from data_loader import load_all_data
from Crawler.tmdb_service import (
    ensure_tmdb_for_movie_ids,
    load_tmdb_map,
    get_poster_base_url,
)

app = Flask(__name__)
app.secret_key = "cse482-secret"  # session signing key (replace in production)

# load data once at startup
data = load_all_data()

movies = data["movies"]                # {movieId: {title, genres}}
user_ratings = data["user_ratings"]    # {userId: [(movieId, rating), ...]}
user_neighbors = data["user_neighbors"]  # {userId: [(neighborId, sim), ...]}
user_recs = data["user_recs"]          # {userId: [(movieId, prediction), ...]}
all_users = data["all_users"]          # sorted list of userIds with data


@app.route("/")
def index():
    return redirect("/login")


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        try:
            user_id = int(request.form["user_id"])  # parse user id from form
        except (KeyError, ValueError):
            return render_template(
                "login.html",
                all_users=all_users,
                error="Please input a valid user id.",
            )

        # accept only users that appear in ratings or recommendations
        if user_id not in user_ratings and user_id not in user_recs:
            return render_template(
                "login.html",
                all_users=all_users,
                error=f"User {user_id} not found.",
            )

        session["user_id"] = user_id  # store logged-in user in session
        return redirect("/dashboard")

    return render_template("login.html", all_users=all_users)


@app.route("/logout")
def logout():
    session.clear()
    return redirect("/login")


@app.route("/dashboard")
def dashboard():
    user_id = session.get("user_id")
    if user_id is None:
        return redirect("/login")

    rated_list = user_ratings.get(user_id, [])   # [(movieId, rating), ...]
    rec_list = user_recs.get(user_id, [])        # [(movieId, prediction), ...]

    # movie ids needed for this page (history + recommendations)
    movie_ids = {mid for mid, _ in rated_list} | {mid for mid, _ in rec_list}

    # ensure TMDB metadata exists for these movies (fetch missing ones)
    ensure_tmdb_for_movie_ids(movie_ids)

    # load TMDB metadata: movieId -> {poster_path, overview, ...}
    tmdb_map = load_tmdb_map(movie_ids)
    poster_base_url = get_poster_base_url()  # e.g. https://image.tmdb.org/t/p/w342/

    rated_view = []
    for mid, rating in rated_list:
        tm = tmdb_map.get(mid, {})
        poster_path = tm.get("poster_path")
        overview = tm.get("overview")

        rated_view.append(
            {
                "movieId": mid,
                "title": movies.get(mid, {}).get("title", f"Movie {mid}"),
                "genres": movies.get(mid, {}).get("genres", ""),
                "rating": rating,
                "ml_url": f"https://movielens.org/movies/{mid}",  # link back to MovieLens
                "poster_path": poster_path,
                "poster_url": f"{poster_base_url}{poster_path}" if poster_path else None,
                "overview": overview,
            }
        )

    rec_view = []
    for mid, pred in rec_list[:30]:  # show top 30 recommendations
        tm = tmdb_map.get(mid, {})
        poster_path = tm.get("poster_path")
        overview = tm.get("overview")

        rec_view.append(
            {
                "movieId": mid,
                "title": movies.get(mid, {}).get("title", f"Movie {mid}"),
                "genres": movies.get(mid, {}).get("genres", ""),
                "prediction": pred,
                "ml_url": f"https://movielens.org/movies/{mid}",
                "poster_path": poster_path,
                "poster_url": f"{poster_base_url}{poster_path}" if poster_path else None,
                "overview": overview,
            }
        )

    neighbor_list = user_neighbors.get(user_id, [])  # similar users for current user
    neighbor_view = [
        {"neighborId": nid, "sim": sim}
        for nid, sim in neighbor_list
    ]

    return render_template(
        "dashboard.html",
        user_id=user_id,
        rated_movies=rated_view,
        recommended_movies=rec_view,
        neighbors=neighbor_view,
    )


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=False)
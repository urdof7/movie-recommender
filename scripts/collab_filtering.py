# scripts/collab_filtering.py

"""
Collaborative filtering script to generate movie recommendations
based on user ratings stored in a SQLite database.
"""

import sqlite3
import os
import pandas as pd
import random
from surprise import Dataset, Reader, SVD
import logging

# Suppress Surprise library output
logging.getLogger('surprise').setLevel(logging.ERROR)

def load_ratings_from_db(db_path):
    """
    Load ratings data from the SQLite database into a Pandas DataFrame.
    """
    conn = sqlite3.connect(db_path)
    query = """
    SELECT user_id, movie_id, rating
    FROM rating;
    """
    ratings_df = pd.read_sql_query(query, conn)
    conn.close()

    # Ensure user_id and movie_id are strings
    ratings_df['user_id'] = ratings_df['user_id'].astype(str)
    ratings_df['movie_id'] = ratings_df['movie_id'].astype(str)

    return ratings_df


def build_collaborative_filtering_model(ratings_df):
    """
    Build and train a collaborative filtering model using the SVD algorithm.
    """
    reader = Reader(rating_scale=(1, 5))
    data = Dataset.load_from_df(ratings_df[['user_id', 'movie_id', 'rating']], reader)
    trainset = data.build_full_trainset()

    # Use the SVD algorithm with adjusted parameters
    algo = SVD(n_factors=50, n_epochs=25, lr_all=0.005, reg_all=0.02, random_state=42)
    algo.fit(trainset)

    return algo


def get_top_n_recommendations(algo, user_id, ratings_df, db_path, n=10):
    """
    Get top N movie recommendations for a given user_id.
    """
    user_id = str(user_id)  # Ensure user_id is a string

    # Get a list of all movie_ids
    all_movie_ids = ratings_df['movie_id'].unique()

    # Get the list of movies the user has already rated
    user_rated_movies = ratings_df[ratings_df['user_id'] == user_id]['movie_id'].tolist()

    # Generate predictions for movies the user hasn't rated yet
    movies_to_predict = [mid for mid in all_movie_ids if mid not in user_rated_movies]

    if not movies_to_predict:
        return []

    # For efficiency, limit the number of movies to predict
    num_movies_to_predict = min(len(movies_to_predict), 1000)
    movies_to_predict_sample = random.sample(list(movies_to_predict), num_movies_to_predict)

    predictions = []
    for mid in movies_to_predict_sample:
        mid = str(mid)  # Ensure movie_id is a string
        pred = algo.predict(user_id, mid)
        predictions.append(pred)

    if not predictions:
        return []

    # Sort the predictions by estimated rating in descending order
    predictions.sort(key=lambda x: x.est, reverse=True)

    # Get the top N recommendations
    top_n_predictions = predictions[:n]

    # Retrieve movie titles from the database
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    recommended_movies = []
    for pred in top_n_predictions:
        movie_id = pred.iid
        cur.execute("SELECT original_title FROM movie WHERE movie_id = ?", (movie_id,))
        result = cur.fetchone()
        if result:
            movie_title = result[0]
            recommended_movies.append({
                'movie_id': movie_id,
                'title': movie_title,
                'estimated_rating': pred.est
            })
    conn.close()

    return recommended_movies


def main():
    # Path to your SQLite database
    db_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'movies.db')

    # Load ratings data from the database
    ratings_df = load_ratings_from_db(db_path)

    # Build the collaborative filtering model
    algo = build_collaborative_filtering_model(ratings_df)

    # Get a list of users who have rated movies
    user_ids = ratings_df['user_id'].unique()

    # Randomly select a user
    user_id = random.choice(user_ids)

    # Get the movies the user has rated
    user_rated_movies = ratings_df[ratings_df['user_id'] == user_id]
    num_rated_movies = len(user_rated_movies)

    # Display user information
    print(f"\nRandomly selected User ID: {user_id}")
    print(f"User {user_id} has rated {num_rated_movies} movies.")

    # Display the movies the user has rated
    print(f"\nMovies rated by user {user_id}:")
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    for idx, row in user_rated_movies.iterrows():
        movie_id = row['movie_id']
        rating = row['rating']
        cur.execute("SELECT original_title FROM movie WHERE movie_id = ?", (movie_id,))
        result = cur.fetchone()
        if result:
            movie_title = result[0]
            print(f"- {movie_title} (Rating: {rating})")
    conn.close()

    # Prompt for the number of recommendations
    while True:
        try:
            top_n = int(input("\nEnter the number of recommendations to display: "))
            if top_n > 0:
                break
            else:
                print("Please enter a positive integer for the number of recommendations.")
        except ValueError:
            print("Invalid input. Please enter an integer.")

    # Get the top N recommendations
    print(f"\nGenerating top {top_n} recommendations for user {user_id}...")
    recommended_movies = get_top_n_recommendations(algo, user_id, ratings_df, db_path, n=top_n)

    # Display the recommendations
    if recommended_movies:
        print("\nTop Recommendations:")
        for idx, movie in enumerate(recommended_movies, start=1):
            print(f"{idx}. {movie['title']} (Estimated Rating: {movie['estimated_rating']:.2f})")
    else:
        print("\nNo personalized recommendations available for this user.")

    # End of script


if __name__ == "__main__":
    main()

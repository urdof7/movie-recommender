"""
Content-based filtering script to generate movie recommendations
based on movie features stored in a SQLite database.
"""

import sqlite3
import os
import pandas as pd
import numpy as np
import random
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import CountVectorizer

def load_movie_features(db_path):
    """
    Load movie features from the SQLite database into a Pandas DataFrame.
    """
    conn = sqlite3.connect(db_path)

    # Load movies
    movies_df = pd.read_sql_query("SELECT movie_id, original_title FROM movie;", conn)

    # Load genres
    genres_df = pd.read_sql_query("""
    SELECT mg.movie_id, g.genre_name
    FROM movie_genre mg
    JOIN genre g ON mg.genre_id = g.genre_id;
    """, conn)

    # Load directors
    directors_df = pd.read_sql_query("""
    SELECT md.movie_id, d.name AS director_name
    FROM movie_director md
    JOIN director d ON md.director_id = d.director_id;
    """, conn)

    # Load cast members
    cast_df = pd.read_sql_query("""
    SELECT mc.movie_id, p.name AS cast_member_name
    FROM movie_cast mc
    JOIN person p ON mc.person_id = p.person_id;
    """, conn)

    conn.close()

    # Merge all features into a single DataFrame
    # Start with movies_df
    movies_df['genres'] = movies_df['movie_id'].map(
        genres_df.groupby('movie_id')['genre_name'].apply(list)
    )
    movies_df['directors'] = movies_df['movie_id'].map(
        directors_df.groupby('movie_id')['director_name'].apply(list)
    )
    movies_df['cast'] = movies_df['movie_id'].map(
        cast_df.groupby('movie_id')['cast_member_name'].apply(list)
    )

    # Replace NaN with empty list
    movies_df['genres'] = movies_df['genres'].apply(lambda x: x if isinstance(x, list) else [])
    movies_df['directors'] = movies_df['directors'].apply(lambda x: x if isinstance(x, list) else [])
    movies_df['cast'] = movies_df['cast'].apply(lambda x: x if isinstance(x, list) else [])

    # Combine features into a single string
    def combine_features(row):
        return ' '.join(row['genres']) + ' ' + ' '.join(row['directors']) + ' ' + ' '.join(row['cast'])

    movies_df['combined_features'] = movies_df.apply(combine_features, axis=1)

    return movies_df

def build_content_based_model(movies_df):
    """
    Build a content-based model using movie features.
    """
    # Create a CountVectorizer to convert the text to a matrix of token counts
    count_vectorizer = CountVectorizer(stop_words='english')
    count_matrix = count_vectorizer.fit_transform(movies_df['combined_features'])

    # Compute the cosine similarity matrix
    cosine_sim_matrix = cosine_similarity(count_matrix, count_matrix)

    return cosine_sim_matrix

def get_top_n_recommendations(user_id, movies_df, cosine_sim_matrix, db_path, n=10):
    """
    Get top N movie recommendations for a given user_id.
    """
    # Connect to the database to get user ratings
    conn = sqlite3.connect(db_path)
    ratings_df = pd.read_sql_query("""
    SELECT user_id, movie_id, rating
    FROM rating;
    """, conn)
    conn.close()

    # Ensure user_id and movie_id are integers
    ratings_df['user_id'] = ratings_df['user_id'].astype(int)
    ratings_df['movie_id'] = ratings_df['movie_id'].astype(int)

    # Get the movies the user has rated
    user_ratings = ratings_df[ratings_df['user_id'] == user_id]

    if user_ratings.empty:
        print(f"No ratings found for user {user_id}.")
        return []

    # Get indices of movies the user has rated
    movie_indices = movies_df[movies_df['movie_id'].isin(user_ratings['movie_id'])].index

    # Get the user's profile by averaging the features of the movies they rated highly
    # Let's consider movies with a rating >= 4.0 as liked by the user
    liked_movies = user_ratings[user_ratings['rating'] >= 4.0]
    liked_movie_indices = movies_df[movies_df['movie_id'].isin(liked_movies['movie_id'])].index

    if liked_movie_indices.empty:
        print(f"User {user_id} has not rated any movies with a rating of 4.0 or higher.")
        return []

    # Calculate the similarity scores
    user_profile = cosine_sim_matrix[liked_movie_indices].mean(axis=0)
    similarity_scores = list(enumerate(user_profile))

    # Sort the movies based on the similarity scores
    similarity_scores = sorted(similarity_scores, key=lambda x: x[1], reverse=True)

    # Get movie indices of movies the user has not rated yet
    unrated_movie_indices = [i for i in range(len(movies_df)) if movies_df.iloc[i]['movie_id'] not in user_ratings['movie_id'].values]

    # Get top N recommendations
    recommendations = []
    for idx, score in similarity_scores:
        if idx in unrated_movie_indices:
            movie_id = movies_df.iloc[idx]['movie_id']
            movie_title = movies_df.iloc[idx]['original_title']
            recommendations.append({
                'movie_id': movie_id,
                'title': movie_title,
                'similarity_score': score
            })
            if len(recommendations) == n:
                break

    return recommendations

def main():
    # Path to your SQLite database
    db_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'movies.db')

    # Load movie features from the database
    print("Loading movie features...")
    movies_df = load_movie_features(db_path)

    # Build the content-based model
    print("Building content-based model...")
    cosine_sim_matrix = build_content_based_model(movies_df)

    # Get a list of users who have rated movies
    conn = sqlite3.connect(db_path)
    ratings_df = pd.read_sql_query("SELECT DISTINCT user_id FROM rating;", conn)
    conn.close()
    user_ids = ratings_df['user_id'].astype(int).tolist()

    if not user_ids:
        print("No users found in the database.")
        return

    # Randomly select a user
    user_id = random.choice(user_ids)

    # Get the movies the user has rated
    conn = sqlite3.connect(db_path)
    user_ratings = pd.read_sql_query(f"""
    SELECT r.movie_id, r.rating, m.original_title
    FROM rating r
    JOIN movie m ON r.movie_id = m.movie_id
    WHERE r.user_id = {user_id};
    """, conn)
    conn.close()

    num_rated_movies = len(user_ratings)

    # Display user information
    print(f"\nRandomly selected User ID: {user_id}")
    print(f"User {user_id} has rated {num_rated_movies} movies.")

    # Display the movies the user has rated
    print(f"\nMovies rated by user {user_id}:")
    for idx, row in user_ratings.iterrows():
        movie_title = row['original_title']
        rating = row['rating']
        print(f"- {movie_title} (Rating: {rating})")

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
    print(f"\nGenerating top {top_n} content-based recommendations for user {user_id}...")
    recommended_movies = get_top_n_recommendations(user_id, movies_df, cosine_sim_matrix, db_path, n=top_n)

    # Display the recommendations
    if recommended_movies:
        print("\nTop Content-Based Recommendations:")
        for idx, movie in enumerate(recommended_movies, start=1):
            print(f"{idx}. {movie['title']} (Similarity Score: {movie['similarity_score']:.4f})")
    else:
        print("\nNo content-based recommendations available for this user.")

    # End of script

if __name__ == "__main__":
    main()

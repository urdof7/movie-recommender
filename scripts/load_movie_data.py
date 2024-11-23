# scripts/load_movie_data.py

"""
Python script to load movie data into a SQLite database defined by movie_schema.sql.
It downloads the original dataset CSV files and loads data from the Kaggle dataset.
"""

import sqlite3
import csv
import requests
import io
from datetime import datetime
import os
import sys

def create_tables(conn):
    """
    Create tables in the SQLite database using the SQL schema.
    """
    schema_path = os.path.join(os.path.dirname(__file__), 'movie_schema.sql')
    with open(schema_path, 'r') as f:
        schema_sql = f.read()

    try:
        cur = conn.cursor()
        cur.executescript(schema_sql)
        conn.commit()
        print("Tables dropped and recreated successfully.")
    except Exception as e:
        print(f"Error creating tables: {e}")
        sys.exit(1)

def load_languages(conn, languages):
    """
    Insert languages into the language table.
    """
    cur = conn.cursor()
    insert_query = """
    INSERT OR IGNORE INTO language (language_code, language_name)
    VALUES (?, ?);
    """
    for code, name in languages.items():
        if code:
            if not name:
                name = 'Unknown'
            cur.execute(insert_query, (code, name))
    conn.commit()

def load_countries(conn, countries):
    """
    Insert countries into the country table.
    """
    cur = conn.cursor()
    insert_query = """
    INSERT OR IGNORE INTO country (country_code, country_name)
    VALUES (?, ?);
    """
    for code, name in countries.items():
        if code:
            if not name:
                name = 'Unknown'
            cur.execute(insert_query, (code, name))
    conn.commit()

def load_genres(conn, genres):
    """
    Insert genres into the genre table.
    """
    cur = conn.cursor()
    insert_query = """
    INSERT OR IGNORE INTO genre (genre_name)
    VALUES (?);
    """
    for genre in genres:
        if genre:
            cur.execute(insert_query, (genre,))
    conn.commit()

def load_production_companies(conn, companies):
    """
    Insert production companies into the production_company table.
    """
    cur = conn.cursor()
    insert_query = """
    INSERT OR IGNORE INTO production_company (company_name)
    VALUES (?);
    """
    for company in companies:
        if company:
            cur.execute(insert_query, (company,))
    conn.commit()

def load_directors(conn, directors):
    """
    Insert directors into the director table.
    """
    cur = conn.cursor()
    insert_query = """
    INSERT OR IGNORE INTO director (name)
    VALUES (?);
    """
    for director in directors:
        if director:
            cur.execute(insert_query, (director,))
    conn.commit()

def load_persons(conn, person_names):
    """
    Insert persons into the person table.
    """
    cur = conn.cursor()
    insert_query = """
    INSERT OR IGNORE INTO person (name)
    VALUES (?);
    """
    for name in person_names:
        if name:
            cur.execute(insert_query, (name,))
    conn.commit()

def load_original_persons(conn, persons_csv_url):
    """
    Insert cast members into the person table from the original dataset.
    """
    cur = conn.cursor()
    response = requests.get(persons_csv_url)
    response.raise_for_status()
    f = io.StringIO(response.text)
    reader = csv.DictReader(f)
    person_names = set()
    for row in reader:
        name = row.get('Name', '').strip()
        if name:
            person_names.add(name)
    load_persons(conn, person_names)

def load_movies(conn, movies_csv_url):
    """
    Insert movies into the movie table and handle related data from the original dataset.
    """
    cur = conn.cursor()
    response = requests.get(movies_csv_url)
    response.raise_for_status()
    f = io.StringIO(response.text)
    reader = csv.DictReader(f)

    # Prepare to collect all unique genres, languages, countries, companies
    genres_set = set()
    languages_dict = {}
    countries_dict = {}
    companies_set = set()

    # First pass: Collect all unique data
    print("Collecting unique languages, countries, genres, and companies from original dataset...")
    for row in reader:
        # Collect languages
        original_language = row.get('OriginalLanguage', '').strip()
        if original_language:
            if '-' in original_language:
                lang_code, lang_name = original_language.split('-', 1)
            else:
                lang_code = original_language
                lang_name = None
            languages_dict[lang_code] = lang_name
        else:
            lang_code = None

        # Collect spoken languages
        spoken_languages = row.get('SpokenLanguages', '').strip()
        if spoken_languages:
            language_list = spoken_languages.split('|')
            for language_entry in language_list:
                if '-' in language_entry:
                    code, name = language_entry.split('-', 1)
                    languages_dict[code] = name

        # Collect production countries
        countries = row.get('ProductionCountries', '').strip()
        if countries:
            country_list = countries.split('|')
            for country_entry in country_list:
                if '-' in country_entry:
                    code, name = country_entry.split('-', 1)
                    countries_dict[code] = name

        # Collect genres
        genres = row.get('Genres', '').strip()
        if genres:
            genre_list = genres.split('|')
            genres_set.update(genre_list)

        # Collect production companies
        companies = row.get('ProductionCompanies', '').strip()
        if companies:
            company_list = companies.split('|')
            companies_set.update(company_list)

    # Load collected data into their respective tables
    load_languages(conn, languages_dict)
    load_countries(conn, countries_dict)
    load_genres(conn, genres_set)
    load_production_companies(conn, companies_set)

    # Reset reader to start from the beginning
    f.seek(0)
    reader = csv.DictReader(f)

    # Second pass: Insert movies and related data
    print("Inserting movies and related data from original dataset...")
    insert_movie_query = """
    INSERT OR IGNORE INTO movie (
        movie_id, original_language_code, original_title, english_title,
        budget, revenue, homepage, runtime, release_date
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
    """

    for row in reader:
        movie_id_str = row.get('MovieID', '').strip()
        if not movie_id_str.isdigit():
            continue  # Skip invalid movie IDs
        movie_id = int(movie_id_str)

        original_language = row.get('OriginalLanguage', '').strip()
        if original_language:
            if '-' in original_language:
                lang_code, lang_name = original_language.split('-', 1)
            else:
                lang_code = original_language
        else:
            lang_code = None

        original_title = row.get('OriginalTitle', '').strip() or None
        english_title = row.get('EnglishTitle', '').strip() or None
        budget_str = row.get('Budget', '').strip()
        budget = float(budget_str) if budget_str else None
        revenue_str = row.get('Revenue', '').strip()
        revenue = float(revenue_str) if revenue_str else None
        homepage = row.get('Homepage', '').strip() or None
        runtime_str = row.get('Runtime', '').strip()
        runtime = int(runtime_str) if runtime_str.isdigit() else None
        release_date_str = row.get('ReleaseDate', '').strip()
        try:
            release_date = datetime.strptime(release_date_str, '%Y-%m-%d').date() if release_date_str else None
        except ValueError:
            release_date = None  # Invalid date format

        # Insert into movie table
        cur.execute(insert_movie_query, (
            movie_id, lang_code, original_title, english_title,
            budget, revenue, homepage, runtime, release_date
        ))

        # Handle genres
        genres = row.get('Genres', '').strip()
        if genres:
            genre_list = genres.split('|')
            for genre in genre_list:
                # Get genre_id from genre_name
                cur.execute("SELECT genre_id FROM genre WHERE genre_name = ?", (genre,))
                result = cur.fetchone()
                if result:
                    genre_id = result[0]
                    # Insert into movie_genre
                    cur.execute("""
                    INSERT OR IGNORE INTO movie_genre (movie_id, genre_id)
                    VALUES (?, ?);
                    """, (movie_id, genre_id))

        # Handle production companies
        companies = row.get('ProductionCompanies', '').strip()
        if companies:
            company_list = companies.split('|')
            for company in company_list:
                # Get company_id from company_name
                cur.execute("SELECT company_id FROM production_company WHERE company_name = ?", (company,))
                result = cur.fetchone()
                if result:
                    company_id = result[0]
                    # Insert into movie_production_company
                    cur.execute("""
                    INSERT OR IGNORE INTO movie_production_company (movie_id, company_id)
                    VALUES (?, ?);
                    """, (movie_id, company_id))

        # Handle production countries
        countries = row.get('ProductionCountries', '').strip()
        if countries:
            country_list = countries.split('|')
            for country_entry in country_list:
                if '-' in country_entry:
                    country_code, country_name = country_entry.split('-', 1)
                    # Insert into production_country
                    cur.execute("""
                    INSERT OR IGNORE INTO production_country (movie_id, country_code)
                    VALUES (?, ?);
                    """, (movie_id, country_code))

        # Handle spoken languages
        spoken_languages = row.get('SpokenLanguages', '').strip()
        if spoken_languages:
            language_list = spoken_languages.split('|')
            for language_entry in language_list:
                if '-' in language_entry:
                    code, name = language_entry.split('-', 1)
                    # Insert into movie_spoken_language
                    cur.execute("""
                    INSERT OR IGNORE INTO movie_spoken_language (movie_id, language_code)
                    VALUES (?, ?);
                    """, (movie_id, code))

    conn.commit()

def load_kaggle_data(conn, kaggle_csv_file):
    """
    Load data from the Kaggle dataset.
    """
    cur = conn.cursor()
    kaggle_csv_path = os.path.join(os.path.dirname(__file__), '..', 'data', kaggle_csv_file)
    if not os.path.exists(kaggle_csv_path):
        print(f"Error: {kaggle_csv_path} not found. Please download it from Kaggle and place it in the data directory.")
        sys.exit(1)

    with open(kaggle_csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f, skipinitialspace=True)

        # Prepare to collect all unique directors, actors, genres
        directors_set = set()
        actors_set = set()
        genres_set = set()

        # First pass: Collect unique data
        print("Collecting unique directors, actors, and genres from Kaggle dataset...")
        for row in reader:
            # Collect directors
            director = row.get('Director', '').strip()
            if director:
                directors_set.add(director)

            # Collect actors
            stars = [
                row.get('Star1', '').strip(),
                row.get('Star2', '').strip(),
                row.get('Star3', '').strip(),
                row.get('Star4', '').strip()
            ]
            actors_set.update(filter(None, stars))

            # Collect genres
            genres = row.get('Genre', '').strip()
            if genres:
                genre_list = [g.strip() for g in genres.split(',')]
                genres_set.update(genre_list)

        # Load collected data into their respective tables
        load_directors(conn, directors_set)
        load_genres(conn, genres_set)
        load_persons(conn, actors_set)

        # Reset file reader
        f.seek(0)
        next(f)  # Skip header line
        reader = csv.DictReader(f, skipinitialspace=True)

        # Second pass: Insert movies and related data
        print("Inserting movies and related data from Kaggle dataset...")
        insert_movie_query = """
        INSERT OR IGNORE INTO movie (
            movie_id, original_title, imdb_rating, meta_score,
            overview, certificate, runtime, release_date, no_of_votes, gross_revenue
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """

        for row in reader:
            # Extract movie title
            movie_title = row.get('Series_Title', '').strip()
            if not movie_title:
                continue  # Skip if movie title is missing

            # Check if movie already exists
            cur.execute("SELECT movie_id FROM movie WHERE original_title = ?", (movie_title,))
            result = cur.fetchone()
            if result:
                continue  # Movie already exists, skip
            else:
                movie_id = cur.execute("SELECT MAX(movie_id) FROM movie").fetchone()[0]
                movie_id = movie_id + 1 if movie_id else 1

            # Extract and process other fields
            try:
                imdb_rating = float(row.get('IMDB_Rating', '').strip()) if row.get('IMDB_Rating', '').strip() else None
            except ValueError:
                imdb_rating = None

            try:
                meta_score = int(row.get('Meta_score', '').strip()) if row.get('Meta_score', '').strip() else None
            except ValueError:
                meta_score = None

            overview = row.get('Overview', '').strip() or None
            certificate = row.get('Certificate', '').strip() or None

            # Handle runtime
            runtime_str = row.get('Runtime', '').strip()
            if 'min' in runtime_str:
                runtime = int(runtime_str.replace('min', '').strip())
            else:
                runtime = None

            # Handle release date
            release_year = row.get('Released_Year', '').strip()
            if release_year.isdigit():
                try:
                    release_date = datetime.strptime(f"{release_year}-01-01", '%Y-%m-%d').date()
                except ValueError:
                    release_date = None
            else:
                release_date = None

            # Handle votes
            no_of_votes_str = row.get('No_of_Votes', '').replace(',', '').strip()
            no_of_votes = int(no_of_votes_str) if no_of_votes_str.isdigit() else None

            # Handle gross revenue
            gross_str = row.get('Gross', '').replace(',', '').replace('$', '').strip()
            try:
                gross_revenue = float(gross_str) if gross_str and gross_str != 'NA' else None
            except ValueError:
                gross_revenue = None

            # Insert movie data
            cur.execute(insert_movie_query, (
                movie_id, movie_title, imdb_rating, meta_score,
                overview, certificate, runtime, release_date, no_of_votes, gross_revenue
            ))

            # Handle genres
            genres = row.get('Genre', '').strip()
            if genres:
                genre_list = [g.strip() for g in genres.split(',')]
                for genre in genre_list:
                    # Get genre_id from genre_name
                    cur.execute("SELECT genre_id FROM genre WHERE genre_name = ?", (genre,))
                    result = cur.fetchone()
                    if result:
                        genre_id = result[0]
                        # Insert into movie_genre
                        cur.execute("""
                        INSERT OR IGNORE INTO movie_genre (movie_id, genre_id)
                        VALUES (?, ?);
                        """, (movie_id, genre_id))

            # Handle directors
            director = row.get('Director', '').strip()
            if director:
                cur.execute("SELECT director_id FROM director WHERE name = ?", (director,))
                result = cur.fetchone()
                if result:
                    director_id = result[0]
                    # Insert into movie_director
                    cur.execute("""
                    INSERT OR IGNORE INTO movie_director (movie_id, director_id)
                    VALUES (?, ?);
                    """, (movie_id, director_id))

            # Handle actors
            stars = [
                row.get('Star1', '').strip(),
                row.get('Star2', '').strip(),
                row.get('Star3', '').strip(),
                row.get('Star4', '').strip()
            ]
            for star in filter(None, stars):
                cur.execute("SELECT person_id FROM person WHERE name = ?", (star,))
                result = cur.fetchone()
                if result:
                    person_id = result[0]
                    # Insert into movie_cast
                    cur.execute("""
                    INSERT OR IGNORE INTO movie_cast (movie_id, person_id)
                    VALUES (?, ?);
                    """, (movie_id, person_id))

    conn.commit()

def load_ratings(conn, ratings_csv_url):
    """
    Insert ratings into the rating table.
    """
    cur = conn.cursor()
    response = requests.get(ratings_csv_url)
    response.raise_for_status()
    f = io.StringIO(response.text)
    reader = csv.DictReader(f)
    insert_user_query = """
    INSERT OR IGNORE INTO user (user_id)
    VALUES (?);
    """
    insert_rating_query = """
    INSERT OR IGNORE INTO rating (user_id, movie_id, rating, rating_date)
    VALUES (?, ?, ?, ?);
    """
    for row in reader:
        user_id_str = row.get('UserID', '').strip()
        movie_id_str = row.get('MovieID', '').strip()
        rating_str = row.get('Rating', '').strip()
        date_str = row.get('Date', '').strip()

        if not (user_id_str.isdigit() and movie_id_str.isdigit() and rating_str):
            continue  # Skip invalid entries

        user_id = int(user_id_str)
        movie_id = int(movie_id_str)
        rating = float(rating_str)
        try:
            rating_date = datetime.strptime(date_str, '%Y-%m-%d').date()
        except ValueError:
            rating_date = None

        # Insert user
        cur.execute(insert_user_query, (user_id,))
        # Insert rating
        cur.execute(insert_rating_query, (user_id, movie_id, rating, rating_date))
    conn.commit()

def main():
    try:
        # Connect to SQLite database
        db_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'movies.db')
        conn = sqlite3.connect(db_path)
        conn.execute('PRAGMA foreign_keys = ON;')  # Enforce foreign key constraints

        # Create tables (drops existing tables first)
        create_tables(conn)

        # GitHub raw URLs for the CSV files
        movies_csv_url = 'https://raw.githubusercontent.com/tugraz-isds/datasets/master/movies/Movies.csv'
        persons_csv_url = 'https://raw.githubusercontent.com/tugraz-isds/datasets/master/movies/Persons.csv'
        ratings_csv_url = 'https://raw.githubusercontent.com/tugraz-isds/datasets/master/movies/Ratings.csv'

        # Load data from the original dataset
        print("Loading persons data from original dataset...")
        load_original_persons(conn, persons_csv_url)
        print("Loading movies data from original dataset...")
        load_movies(conn, movies_csv_url)
        print("Loading ratings data from original dataset...")
        load_ratings(conn, ratings_csv_url)

        # Load Kaggle data
        print("Loading data from Kaggle dataset...")
        kaggle_csv_file = 'imdb_top_1000.csv'  # Ensure this file is placed in the data directory
        load_kaggle_data(conn, kaggle_csv_file)

        print("Data loaded successfully.")

    except Exception as e:
        print(f"Error loading data: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()

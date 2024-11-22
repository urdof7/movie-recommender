# load_movie_data.py

"""
Python script to load movie data into a SQLite database defined by movie_schema.sql.
It downloads the CSV files directly from the GitHub repository.
"""

import sqlite3
import csv
import requests
import io
from datetime import datetime

def create_tables(conn):
    """
    Create tables in the SQLite database using the SQL schema.
    """
    with open('movie_schema.sql', 'r') as f:
        schema_sql = f.read()

    try:
        cur = conn.cursor()
        cur.executescript(schema_sql)
        conn.commit()
        print("Tables dropped and recreated successfully.")
    except Exception as e:
        print(f"Error creating tables: {e}")

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

def load_persons(conn, persons_csv_url):
    """
    Insert cast members into the person table.
    """
    cur = conn.cursor()
    response = requests.get(persons_csv_url)
    response.raise_for_status()
    f = io.StringIO(response.text)
    reader = csv.DictReader(f)
    insert_query = """
    INSERT OR IGNORE INTO person (cast_id, name, gender)
    VALUES (?, ?, ?);
    """
    for row in reader:
        cast_id = row['CastID']
        name = row['Name']
        gender = row['Gender']
        gender = int(gender) if gender.isdigit() else None
        if cast_id and name:
            cur.execute(insert_query, (cast_id, name, gender))
    conn.commit()

def load_movies(conn, movies_csv_url):
    """
    Insert movies into the movie table and handle related data.
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

    # First pass: Collect all unique languages, countries, genres, companies
    print("Collecting unique languages, countries, genres, and companies...")
    for row in reader:
        # Collect languages
        original_language = row['OriginalLanguage']
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
        spoken_languages = row['SpokenLanguages']
        if spoken_languages:
            language_list = spoken_languages.split('|')
            for language_entry in language_list:
                if '-' in language_entry:
                    code, name = language_entry.split('-', 1)
                    languages_dict[code] = name

        # Collect production countries
        countries = row['ProductionCountries']
        if countries:
            country_list = countries.split('|')
            for country_entry in country_list:
                if '-' in country_entry:
                    code, name = country_entry.split('-', 1)
                    countries_dict[code] = name

        # Collect genres
        genres = row['Genres']
        if genres:
            genre_list = genres.split('|')
            genres_set.update(genre_list)

        # Collect production companies
        companies = row['ProductionCompanies']
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
    print("Inserting movies and related data...")
    insert_movie_query = """
    INSERT OR IGNORE INTO movie (
        movie_id, original_language_code, original_title, english_title,
        budget, revenue, homepage, runtime, release_date
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
    """

    for row in reader:
        movie_id = int(row['MovieID'])
        original_language = row['OriginalLanguage']
        if original_language:
            if '-' in original_language:
                lang_code, lang_name = original_language.split('-', 1)
            else:
                lang_code = original_language
        else:
            lang_code = None

        original_title = row['OriginalTitle']
        english_title = row['EnglishTitle']
        budget = row['Budget']
        budget = float(budget) if budget else None
        revenue = row['Revenue']
        revenue = float(revenue) if revenue else None
        homepage = row['Homepage']
        runtime = row['Runtime']
        runtime = int(runtime) if runtime else None
        release_date = row['ReleaseDate']
        release_date = datetime.strptime(release_date, '%Y-%m-%d').date() if release_date else None

        # Insert into movie table
        cur.execute(insert_movie_query, (
            movie_id, lang_code, original_title, english_title,
            budget, revenue, homepage, runtime, release_date
        ))

        # Handle genres
        genres = row['Genres']
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
        companies = row['ProductionCompanies']
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
        countries = row['ProductionCountries']
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
        spoken_languages = row['SpokenLanguages']
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

        # Handle cast IDs
        cast_ids = row['CastID']
        if cast_ids:
            cast_id_list = cast_ids.split('|')
            for cast_id in cast_id_list:
                # Insert into movie_cast
                cur.execute("""
                INSERT OR IGNORE INTO movie_cast (movie_id, cast_id)
                VALUES (?, ?);
                """, (movie_id, cast_id))

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
        user_id = int(row['UserID'])
        movie_id = int(row['MovieID'])
        rating = float(row['Rating'])
        rating_date = datetime.strptime(row['Date'], '%Y-%m-%d').date()
        # Insert user
        cur.execute(insert_user_query, (user_id,))
        # Insert rating
        cur.execute(insert_rating_query, (user_id, movie_id, rating, rating_date))
    conn.commit()

def main():
    try:
        # Connect to SQLite database
        conn = sqlite3.connect('movies.db')
        conn.execute('PRAGMA foreign_keys = ON;')  # Enforce foreign key constraints

        # Create tables (drops existing tables first)
        create_tables(conn)

        # GitHub raw URLs for the CSV files
        movies_csv_url = 'https://raw.githubusercontent.com/tugraz-isds/datasets/master/movies/Movies.csv'
        persons_csv_url = 'https://raw.githubusercontent.com/tugraz-isds/datasets/master/movies/Persons.csv'
        ratings_csv_url = 'https://raw.githubusercontent.com/tugraz-isds/datasets/master/movies/Ratings.csv'

        # Load data
        print("Loading persons data...")
        load_persons(conn, persons_csv_url)
        print("Loading movies data...")
        load_movies(conn, movies_csv_url)
        print("Loading ratings data...")
        load_ratings(conn, ratings_csv_url)

        print("Data loaded successfully.")

    except Exception as e:
        print(f"Error loading data: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()

-- scripts/movie_schema.sql

-- Schema to store movies, cast members, ratings, and related data.

-- Drop existing tables if they exist, ensuring a fresh start.
PRAGMA foreign_keys = OFF;

DROP TABLE IF EXISTS rating;
DROP TABLE IF EXISTS movie_cast;
DROP TABLE IF EXISTS movie_spoken_language;
DROP TABLE IF EXISTS production_country;
DROP TABLE IF EXISTS movie_production_company;
DROP TABLE IF EXISTS movie_genre;
DROP TABLE IF EXISTS movie_director;
DROP TABLE IF EXISTS movie;
DROP TABLE IF EXISTS person;
DROP TABLE IF EXISTS user;
DROP TABLE IF EXISTS production_company;
DROP TABLE IF EXISTS director;
DROP TABLE IF EXISTS genre;
DROP TABLE IF EXISTS country;
DROP TABLE IF EXISTS language;

PRAGMA foreign_keys = ON;

-- Table: language

CREATE TABLE language (
    language_code VARCHAR(10) PRIMARY KEY,
    language_name VARCHAR(255) NOT NULL
);

-- Table: country

CREATE TABLE country (
    country_code VARCHAR(10) PRIMARY KEY,
    country_name VARCHAR(255) NOT NULL
);

-- Table: genre

CREATE TABLE genre (
    genre_id INTEGER PRIMARY KEY AUTOINCREMENT,
    genre_name VARCHAR(50) UNIQUE NOT NULL
);

-- Table: production_company

CREATE TABLE production_company (
    company_id INTEGER PRIMARY KEY AUTOINCREMENT,
    company_name VARCHAR(255) UNIQUE NOT NULL
);

-- Table: director

CREATE TABLE director (
    director_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(255) UNIQUE NOT NULL
);

-- Table: user

CREATE TABLE user (
    user_id INT PRIMARY KEY
    -- Additional user information can be added here if needed
);

-- Table: person (Cast Members and Stars)

CREATE TABLE person (
    person_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(255) UNIQUE NOT NULL
    -- Additional person information can be added here if needed
);

-- Table: movie

CREATE TABLE movie (
    movie_id INT PRIMARY KEY,
    original_language_code VARCHAR(10),
    original_title VARCHAR(255) NOT NULL,
    english_title VARCHAR(255),
    budget REAL CHECK (budget >= 0),
    revenue REAL CHECK (revenue >= 0),
    homepage VARCHAR(255),
    runtime INT CHECK (runtime > 0),
    release_date DATE,
    imdb_rating REAL CHECK (imdb_rating >= 0 AND imdb_rating <= 10),
    meta_score INT CHECK (meta_score >= 0 AND meta_score <= 100),
    overview TEXT,
    certificate VARCHAR(20),
    no_of_votes INT CHECK (no_of_votes >= 0),
    gross_revenue REAL CHECK (gross_revenue >= 0),
    FOREIGN KEY (original_language_code) REFERENCES language(language_code)
);

-- Table: movie_genre (Association between Movies and Genres)

CREATE TABLE movie_genre (
    movie_id INT,
    genre_id INT,
    PRIMARY KEY (movie_id, genre_id),
    FOREIGN KEY (movie_id) REFERENCES movie(movie_id),
    FOREIGN KEY (genre_id) REFERENCES genre(genre_id)
);

-- Table: movie_production_company (Association between Movies and Production Companies)

CREATE TABLE movie_production_company (
    movie_id INT,
    company_id INT,
    PRIMARY KEY (movie_id, company_id),
    FOREIGN KEY (movie_id) REFERENCES movie(movie_id),
    FOREIGN KEY (company_id) REFERENCES production_company(company_id)
);

-- Table: production_country (Association between Movies and Countries)

CREATE TABLE production_country (
    movie_id INT,
    country_code VARCHAR(10),
    PRIMARY KEY (movie_id, country_code),
    FOREIGN KEY (movie_id) REFERENCES movie(movie_id),
    FOREIGN KEY (country_code) REFERENCES country(country_code)
);

-- Table: movie_spoken_language (Association between Movies and Languages)

CREATE TABLE movie_spoken_language (
    movie_id INT,
    language_code VARCHAR(10),
    PRIMARY KEY (movie_id, language_code),
    FOREIGN KEY (movie_id) REFERENCES movie(movie_id),
    FOREIGN KEY (language_code) REFERENCES language(language_code)
);

-- Table: movie_cast (Association between Movies and Cast Members)

CREATE TABLE movie_cast (
    movie_id INT,
    person_id INT,
    character_name VARCHAR(255),
    PRIMARY KEY (movie_id, person_id),
    FOREIGN KEY (movie_id) REFERENCES movie(movie_id),
    FOREIGN KEY (person_id) REFERENCES person(person_id)
);

-- Table: movie_director (Association between Movies and Directors)

CREATE TABLE movie_director (
    movie_id INT,
    director_id INT,
    PRIMARY KEY (movie_id, director_id),
    FOREIGN KEY (movie_id) REFERENCES movie(movie_id),
    FOREIGN KEY (director_id) REFERENCES director(director_id)
);

-- Table: rating

CREATE TABLE rating (
    user_id INT,
    movie_id INT,
    rating REAL CHECK (rating >= 0.5 AND rating <= 5.0 AND (rating * 2) % 1 = 0),
    rating_date DATE,
    PRIMARY KEY (user_id, movie_id),
    FOREIGN KEY (user_id) REFERENCES user(user_id),
    FOREIGN KEY (movie_id) REFERENCES movie(movie_id)
);

-- Indexes for Optimizing Joins and Query Performance
-- These indexes are chosen to improve the efficiency of joins across the schema, especially for tables
-- involved in foreign key relationships and many-to-many associations. They target the most common
-- query patterns such as filtering, sorting, and joining, ensuring minimal full table scans and faster
-- data retrieval.
-- Index for fast searching of movies by title
CREATE INDEX idx_movie_title ON movie (original_title);

-- Index for optimizing queries by user in the rating table
CREATE INDEX idx_rating_user ON rating (user_id);

-- Index for optimizing queries by movie in the rating table
CREATE INDEX idx_rating_movie ON rating (movie_id);

-- Index for improving performance of genre-based queries
CREATE INDEX idx_movie_genre ON movie_genre (genre_id);

-- Index for linking movies to directors efficiently
CREATE INDEX idx_movie_director ON movie_director (director_id);

-- Index for filtering or sorting movies by release date
CREATE INDEX idx_movie_release_date ON movie (release_date);

-- Index for looking up cast members by name
CREATE INDEX idx_person_name ON person (name);

-- Index for filtering movies by original language
CREATE INDEX idx_movie_language ON movie (original_language_code);



-- View: Movies and their associated genres
CREATE VIEW movies_and_genres AS
SELECT 
    m.movie_id,
    m.original_title AS movie_title,
    g.genre_name AS genre
FROM 
    movie m
JOIN 
    movie_genre mg ON m.movie_id = mg.movie_id
JOIN 
    genre g ON mg.genre_id = g.genre_id;

-- View: User activity summary (total ratings and average rating)
CREATE VIEW user_activity_summary AS
SELECT 
    u.user_id,
    COUNT(r.movie_id) AS total_ratings,
    AVG(r.rating) AS average_rating
FROM 
    user u
JOIN 
    rating r ON u.user_id = r.user_id
GROUP BY 
    u.user_id;

-- View: Top-rated movies with high IMDB ratings
CREATE VIEW top_rated_movies AS
SELECT 
    movie_id,
    original_title AS movie_title,
    imdb_rating,
    release_date
FROM 
    movie
WHERE 
    imdb_rating >= 8.0
ORDER BY 
    imdb_rating DESC;

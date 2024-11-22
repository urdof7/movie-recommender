-- movie_schema.sql

-- Schema to store movies, cast members, ratings, and related data.

-- Drop existing tables if they exist, ensuring a fresh start.
PRAGMA foreign_keys = OFF;

DROP TABLE IF EXISTS rating;
DROP TABLE IF EXISTS movie_cast;
DROP TABLE IF EXISTS movie_spoken_language;
DROP TABLE IF EXISTS production_country;
DROP TABLE IF EXISTS movie_production_company;
DROP TABLE IF EXISTS movie_genre;
DROP TABLE IF EXISTS movie;
DROP TABLE IF EXISTS person;
DROP TABLE IF EXISTS user;
DROP TABLE IF EXISTS production_company;
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

-- Table: user

CREATE TABLE user (
    user_id INT PRIMARY KEY
);

-- Table: person (Cast Members)

CREATE TABLE person (
    cast_id VARCHAR(24) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    gender INT,
    CHECK (gender IN (1, 2) OR gender IS NULL)
);

-- Table: movie

CREATE TABLE movie (
    movie_id INT PRIMARY KEY,
    original_language_code VARCHAR(2),
    original_title VARCHAR(255) NOT NULL,
    english_title VARCHAR(255),
    budget DECIMAL(15,2) CHECK (budget >= 0),
    revenue DECIMAL(15,2) CHECK (revenue >= 0),
    homepage VARCHAR(255),
    runtime INT CHECK (runtime > 0),
    release_date DATE,
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
    country_code VARCHAR(2),
    PRIMARY KEY (movie_id, country_code),
    FOREIGN KEY (movie_id) REFERENCES movie(movie_id),
    FOREIGN KEY (country_code) REFERENCES country(country_code)
);

-- Table: movie_spoken_language (Association between Movies and Languages)

CREATE TABLE movie_spoken_language (
    movie_id INT,
    language_code VARCHAR(2),
    PRIMARY KEY (movie_id, language_code),
    FOREIGN KEY (movie_id) REFERENCES movie(movie_id),
    FOREIGN KEY (language_code) REFERENCES language(language_code)
);

-- Table: movie_cast (Association between Movies and Cast Members)

CREATE TABLE movie_cast (
    movie_id INT,
    cast_id VARCHAR(24),
    character_name VARCHAR(255),
    PRIMARY KEY (movie_id, cast_id),
    FOREIGN KEY (movie_id) REFERENCES movie(movie_id),
    FOREIGN KEY (cast_id) REFERENCES person(cast_id)
);

-- Table: rating

CREATE TABLE rating (
    user_id INT,
    movie_id INT,
    rating DECIMAL(2,1) CHECK (rating >= 0.5 AND rating <= 5.0 AND (rating * 2) % 1 = 0),
    rating_date DATE,
    PRIMARY KEY (user_id, movie_id),
    FOREIGN KEY (user_id) REFERENCES user(user_id),
    FOREIGN KEY (movie_id) REFERENCES movie(movie_id)
);

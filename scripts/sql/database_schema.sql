DROP TABLE IF EXISTS lang CASCADE;
DROP TABLE IF EXISTS subject CASCADE;
DROP TABLE IF EXISTS author CASCADE;
DROP TABLE IF EXISTS publisher CASCADE;
DROP TABLE IF EXISTS library_user CASCADE;
DROP TABLE IF EXISTS work_subject CASCADE;
DROP TABLE IF EXISTS work_author CASCADE;
DROP TABLE IF EXISTS loan_return CASCADE;
DROP TABLE IF EXISTS loan CASCADE;
DROP TABLE IF EXISTS inventory_item CASCADE;
DROP TABLE IF EXISTS listing CASCADE;
DROP TABLE IF EXISTS rating CASCADE;
DROP TABLE IF EXISTS work CASCADE;
DROP TABLE IF EXISTS pfp CASCADE;

DROP DOMAIN IF EXISTS EMAIL_DOMAIN CASCADE;
DROP DOMAIN IF EXISTS gender_domain;
DROP DOMAIN IF EXISTS USER_ROLE_DOMAIN;

CREATE EXTENSION IF NOT EXISTS citext;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

DROP TYPE IF EXISTS ITEM_MEDIUM_TYPE;
DROP TYPE IF EXISTS READING_STATUS_TYPE;

CREATE DOMAIN GENDER_DOMAIN CHAR(1) CHECK (LOWER(VALUE) IN ('f', 'm', 'n'));

CREATE DOMAIN EMAIL_DOMAIN AS citext CHECK (
    VALUE ~ '^[a-zA-Z0-9.!#$%&''*+/=?^_`{|}~-]+@[a-zA-Z0-9-]+(?:\.[a-zA-Z0-9-]+)*$'
);

CREATE DOMAIN USER_ROLE_DOMAIN AS TEXT
  CHECK (VALUE IN ('USER', 'ADMIN'));

CREATE TYPE ITEM_MEDIUM_TYPE AS ENUM (
    'EBOOK',
    'BOOK',
    'AUDIOBOOK'
);

CREATE TYPE READING_STATUS_TYPE AS ENUM (
    'ALREADY_READ',
    'CURRENTLY_READING',
    'WANT_TO_READ'
);

CREATE TABLE IF NOT EXISTS library_user (
    user_id SERIAL PRIMARY KEY,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    gender GENDER_DOMAIN NOT NULL,
    email EMAIL_DOMAIN UNIQUE NOT NULL,
    birthday DATE NOT NULL,
    role USER_ROLE_DOMAIN NOT NULL,
    modified_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pfp (
    user_id INTEGER PRIMARY KEY REFERENCES library_user(user_id) ON DELETE CASCADE,
    pfp_url TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS author (
    author_id SERIAL PRIMARY KEY,
    full_name TEXT UNIQUE NOT NULL,
    modified_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS publisher (
    publisher_id SERIAL PRIMARY KEY,
    publisher_name TEXT UNIQUE NOT NULL,
    modified_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS lang (
    language_id VARCHAR(3) PRIMARY KEY,
    lang_name TEXT NOT NULL,
    speakers INTEGER NOT NULL,
    modified_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS work (
    work_id SERIAL PRIMARY KEY,
    publisher_id INTEGER NOT NULL REFERENCES publisher(publisher_id) ON DELETE CASCADE,
    isbn VARCHAR(13) UNIQUE NOT NULL,
    language_id VARCHAR(3) REFERENCES lang(language_id) ON DELETE CASCADE,
    title TEXT NOT NULL,
    pages INTEGER NOT NULL,
    weight float NOT NULL,
    release_year INTEGER NOT NULL,
    modified_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS work_author (
    work_id INTEGER REFERENCES work(work_id) ON DELETE CASCADE,
    author_id INTEGER REFERENCES author(author_id) ON DELETE CASCADE,
    added_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY(work_id, author_id)
);

CREATE TABLE IF NOT EXISTS subject (
    subject_id SERIAL PRIMARY KEY,
    subject_name TEXT UNIQUE NOT NULL,
    modified_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS work_subject(
    work_id INTEGER REFERENCES work(work_id) ON DELETE CASCADE,
    subject_id INTEGER REFERENCES subject(subject_id) ON DELETE CASCADE,
    PRIMARY KEY(work_id, subject_id)
);

CREATE TABLE IF NOT EXISTS rating (
    rating_id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES library_user(user_id) ON DELETE CASCADE,
    work_id INTEGER REFERENCES work(work_id) ON DELETE CASCADE,
    score INTEGER NOT NULL,
    rated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS listing (
    listing_id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES library_user(user_id) ON DELETE CASCADE,
    work_id INTEGER REFERENCES work(work_id) ON DELETE CASCADE,
    reading_status READING_STATUS_TYPE NOT NULL,
    listed_at TIMESTAMP NOT NULL DEFAULT NOW()
);


CREATE TABLE IF NOT EXISTS inventory_item(
    item_id SERIAL PRIMARY KEY,
    work_id INTEGER REFERENCES work(work_id),
    medium ITEM_MEDIUM_TYPE NOT NULL
);


CREATE TABLE IF NOT EXISTS loan(
    loan_id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES library_user(user_id) ON DELETE CASCADE,
    item_id INTEGER REFERENCES inventory_item(item_id) ON DELETE CASCADE,
    loaned_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS loan_return(
    loan_id INTEGER PRIMARY KEY REFERENCES loan(loan_id),
    returned_at TIMESTAMP NOT NULL DEFAULT NOW()
);


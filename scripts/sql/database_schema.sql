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

CREATE EXTENSION IF NOT EXISTS citext;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

DROP TYPE IF EXISTS ITEM_MEDIUM_TYPE;
DROP TYPE IF EXISTS READING_STATUS_TYPE;

CREATE DOMAIN GENDER_DOMAIN CHAR(1) CHECK (LOWER(VALUE) IN ('f', 'm', 'n'));

CREATE DOMAIN EMAIL_DOMAIN AS citext CHECK (
    VALUE ~ '^[a-zA-Z0-9.!#$%&''*+/=?^_`{|}~-]+@[a-zA-Z0-9-]+(?:\.[a-zA-Z0-9-]+)*$'
);

CREATE TYPE ITEM_MEDIUM_TYPE AS ENUM (
    'EBOOK',
    'BOOK',
    'AUDIOBOOK',
    'BOOK, ER'
);

CREATE TYPE READING_STATUS_TYPE AS ENUM (
    'ALREADY_READ',
    'CURRENTLY_READING',
    'WANT_TO_READ'
);

CREATE OR REPLACE FUNCTION CHECK_release_year() RETURNS TRIGGER AS $$
BEGIN
    IF NEW.release_year > EXTRACT(YEAR FROM NOW()) THEN
        RAISE EXCEPTION 'Release year cannot be in the future';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE IF NOT EXISTS library_user (
    user_id SERIAL PRIMARY KEY,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    gender GENDER_DOMAIN NOT NULL,
    email EMAIL_DOMAIN UNIQUE NOT NULL,
    birthday DATE NOT NULL,
    added_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pfp (
    user_id INTEGER PRIMARY KEY REFERENCES library_user(user_id) ON DELETE CASCADE,
    pfp_url TEXT NOT NULL
);

CREATE INDEX idx_library_user ON pfp (user_id);

CREATE TABLE IF NOT EXISTS author (
    author_id SERIAL PRIMARY KEY,
    full_name TEXT UNIQUE NOT NULL,
    added_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_author_full_name ON author USING gin(full_name gin_trgm_ops);

CREATE TABLE IF NOT EXISTS publisher (
    publisher_id SERIAL PRIMARY KEY,
    publisher_name UNIQUE TEXT NOT NULL
);
CREATE INDEX idx_publisher_name ON publisher USING gin(publisher_name gin_trgm_ops);

CREATE TABLE IF NOT EXISTS lang (
    language_id VARCHAR(3) PRIMARY KEY CHECK (CHAR_LENGTH(language_id) = 3),
    lang_name TEXT NOT NULL,
    speakers bigINTEGER NOT NULL CHECK (speakers >= 0),
    added_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS work (
    work_id SERIAL PRIMARY KEY,
    publisher_id INTEGER NOT NULL REFERENCES publisher(publisher_id) ON DELETE CASCADE,
    isbn VARCHAR(13) UNIQUE NOT NULL CHECK (LENGTH(isbn) = 13),
    language_id VARCHAR(3) REFERENCES lang(language_id) ON DELETE CASCADE,
    title TEXT NOT NULL,
    pages INTEGER NOT NULL CHECK (pages >= 0),
    weight float NOT NULL CHECK (weight >= 0),
    release_year INTEGER NOT NULL,
    added_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_work_publisher ON work (publisher_id);
CREATE INDEX idx_work_language ON work(language_id);
CREATE INDEX idx_isbn ON work USING gin(isbn gin_trgm_ops);
CREATE INDEX idx_title ON work USING gin(title gin_trgm_ops);

CREATE TRIGGER CHECK_year BEFORE
INSERT
    OR
UPDATE
    ON work FOR EACH ROW EXECUTE PROCEDURE CHECK_release_year();

CREATE TABLE IF NOT EXISTS work_author (
    work_id INTEGER REFERENCES work(work_id) ON DELETE CASCADE,
    author_id INTEGER REFERENCES author(author_id) ON DELETE CASCADE,
    PRIMARY KEY(work_id, author_id)
);
CREATE INDEX idx_work_author_work ON work_author (work_id);
CREATE INDEX idx_work_author_author ON work_author (author_id);

CREATE TABLE IF NOT EXISTS subject (
    subject_id SERIAL PRIMARY KEY,
    subject_name UNIQUE TEXT NOT NULL
);
CREATE INDEX idx_subject_name ON subject USING gin(subject_name gin_trgm_ops);

CREATE TABLE IF NOT EXISTS work_subject(
    work_id INTEGER REFERENCES work(work_id) ON DELETE CASCADE,
    subject_id INTEGER REFERENCES subject(subject_id) ON DELETE CASCADE,
    PRIMARY KEY(work_id, subject_id)
);
CREATE INDEX idx_work_subject_work ON work_subject (work_id);
CREATE INDEX idx_work_subject_subject ON work_subject (subject_id);

CREATE TABLE IF NOT EXISTS rating (
    rating_id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES library_user(user_id) ON DELETE CASCADE,
    work_id INTEGER REFERENCES work(work_id) ON DELETE CASCADE,
    score INTEGER NOT NULL CHECK (
        0 < score
        and score <= 5
    ),
    rated_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_rating_user ON rating (user_id);
CREATE INDEX idx_rating_work ON rating (work_id);

CREATE TABLE IF NOT EXISTS listing (
    listing_id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES library_user(user_id) ON DELETE CASCADE,
    work_id INTEGER REFERENCES work(work_id) ON DELETE CASCADE,
    reading_status READING_STATUS_TYPE NOT NULL,
    listed_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_listing_user ON listing (user_id);
CREATE INDEX idx_listing_work ON listing (work_id);
CREATE INDEX idx_reading_status ON listing (reading_status);


CREATE TABLE IF NOT EXISTS inventory_item(
    item_id SERIAL PRIMARY KEY,
    work_id INTEGER REFERENCES work(work_id),
    medium ITEM_MEDIUM_TYPE NOT NULL,
    added_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_inventory_item_work ON inventory_item (work_id);
CREATE INDEX idx_item_medium_type ON inventory_item (medium);


CREATE TABLE IF NOT EXISTS loan(
    loan_id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES library_user(user_id) ON DELETE CASCADE,
    item_id INTEGER REFERENCES inventory_item(item_id) ON DELETE CASCADE,
    loaned_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_loan_user ON loan (user_id);
CREATE INDEX idx_loan_item ON loan (item_id);

CREATE TABLE IF NOT EXISTS loan_return(
    loan_id INTEGER PRIMARY KEY REFERENCES loan(loan_id),
    returned_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_loan_return ON loan_return (loan_id);
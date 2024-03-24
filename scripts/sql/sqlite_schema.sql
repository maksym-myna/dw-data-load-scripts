DROP TABLE IF EXISTS work_isbn;
DROP TABLE IF EXISTS work_subject;
DROP TABLE IF EXISTS subject;
DROP TABLE IF EXISTS work_id;
DROP TABLE IF EXISTS work;
DROP TABLE IF EXISTS work_author;
DROP TABLE IF EXISTS author_id;
DROP TABLE IF EXISTS clustered_subject;
DROP TABLE IF EXISTS subject_to_clustered_subject;
DROP TABLE IF EXISTS publisher;
DROP TABLE IF EXISTS processed_publisher;
DROP TABLE IF EXISTS publisher_id;

CREATE TABLE IF NOT EXISTS work_isbn (
    work_id INTEGER,
    isbn TEXT,
    PRIMARY KEY(work_id, isbn)
);

CREATE INDEX IF NOT EXISTS idx_work_id ON work_isbn(work_id);

CREATE INDEX IF NOT EXISTS idx_isbn ON work_isbn(isbn);

CREATE TABLE subject(
    subject_id INTEGER PRIMARY KEY,
    subject_name TEXT UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS subject_idx_work_id ON subject(subject_name);

CREATE TABLE work_subject(
    work_id INTEGER,
    subject_name TEXT NOT NULL,
    PRIMARY KEY(work_id, subject_name)
);

CREATE INDEX IF NOT EXISTS work_subject_idx_work_id ON work_subject(work_id);

CREATE INDEX IF NOT EXISTS work_subject_idx_isbn ON work_subject(subject_name);

CREATE TABLE clustered_subject(
    clustered_subject_id INTEGER PRIMARY KEY,
    subject_name TEXT UNIQUE NOT NULL
);

CREATE TABLE subject_to_clustered_subject(
    subject_id INTEGER PRIMARY KEY,
    clustered_subject_id INTEGER
);

CREATE INDEX IF NOT EXISTS clustered_subject_idx ON subject_to_clustered_subject(clustered_subject_id);

CREATE TABLE IF NOT EXISTS work_id (
    work_id INTEGER
);

CREATE TABLE IF NOT EXISTS work_author (
    work_id INTEGER,
    author_id INTEGER,
    PRIMARY KEY(work_id, author_id)
);

CREATE INDEX IF NOT EXISTS work_author_idx_work_id ON work_author(work_id);

CREATE INDEX IF NOT EXISTS work_author_idx_author_id ON work_author(author_id);

CREATE TABLE IF NOT EXISTS author_id (author_id INTEGER PRIMARY KEY);

CREATE TABLE IF NOT EXISTS publisher (
    publisher_id INTEGER,
    publisher_name TEXT NOT NULL,
    processed_publisher_id INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS processed_publisher (
    processed_publisher_id INTEGER PRIMARY KEY AUTOINCREMENT,
    processed_publisher_name TEXT NOT NULL
);

PRAGMA journal_mode=MEMORY;
PRAGMA synchronous=OFF;
PRAGMA cache_size=-10000;

CREATE TABLE IF NOT EXISTS work_isbn (
    work_id INTEGER PRIMARY KEY,
    isbn TEXT
);
CREATE INDEX IF NOT EXISTS idx_work_id ON work_isbn(work_id);
CREATE INDEX IF NOT EXISTS idx_isbn ON work_isbn(isbn);

CREATE TABLE /iF NOT EXISTS work_subject(
    work_id INTEGER,
    subject_name TEXT NOT NULL,
    PRIMARY KEY(work_id, subject_name)
);
CREATE INDEX IF NOT EXISTS work_subject_idx_work_id ON work_subject(work_id);

CREATE TABLE IF NOT EXISTS publisher (
    publisher_id INTEGER,
    publisher_name TEXT NOT NULL,
    processed_publisher_id INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS processed_publisher (
    processed_publisher_id INTEGER PRIMARY KEY AUTOINCREMENT,
    processed_publisher_name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS work_id (
    work_id INTEGER
);

CREATE TABLE IF NOT EXISTS author_id(
    author_id INTEGER PRIMARY KEY
);

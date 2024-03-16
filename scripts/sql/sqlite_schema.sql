DROP TABLE IF EXISTS work_isbn;

DROP TABLE IF EXISTS work_subject;

DROP TABLE IF EXISTS subject;

DROP TABLE IF EXISTS work_id;

DROP TABLE IF EXISTS work_author;

DROP TABLE IF EXISTS author_id;

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

CREATE TABLE IF NOT EXISTS work_id (
    old_id TEXT,
    work_id INTEGER PRIMARY KEY AUTOINCREMENT
);

CREATE TABLE IF NOT EXISTS work_author (
    work_id INTEGER,
    author_id INTEGER,
    PRIMARY KEY(work_id, author_id)
);

CREATE INDEX IF NOT EXISTS work_author_idx_work_id ON work_author(work_id);

CREATE INDEX IF NOT EXISTS work_author_idx_author_id ON work_author(author_id);

CREATE TABLE IF NOT EXISTS author_id (author_id INTEGER PRIMARY KEY)
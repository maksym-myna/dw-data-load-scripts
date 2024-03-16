DROP TABLE work_isbn;

DROP TABLE work_weight;

DROP TABLE work_language;

DROP TABLE lang;

DROP TABLE work_subject;

DROP TABLE subject;

DROP TABLE work_author;

DROP TABLE author;

DROP TABLE loan_return;

DROP TABLE loan;

DROP TABLE inventory_item;

DROP TABLE listing;

DROP TABLE rating;

DROP TABLE publisher;

DROP TABLE work;

DROP TABLE pfp;

DROP TABLE library_user;

CREATE DOMAIN IF NOT EXISTS gender_domain CHAR(1) CHECK (LOWER(VALUE) IN ('f', 'm', 'n'));

CREATE EXTENSION IF NOT EXISTS citext;

CREATE DOMAIN IF NOT EXISTS email_domain AS citext CHECK (
    VALUE ~ '^[a-zA-Z0-9.!#$%&''*+/=?^_`{|}~-]+@[a-zA-Z0-9-]+(?:\.[a-zA-Z0-9-]+)*$'
);

CREATE
OR REPLACE FUNCTION check_release_year() RETURNS TRIGGER AS $ $ BEGIN IF NEW.release_year > EXTRACT(
    YEAR
    FROM
        NOW()
) THEN RAISE EXCEPTION 'Release year cannot be in the future';

END IF;

RETURN NEW;

END;

$ $ LANGUAGE plpgsql;

CREATE TABLE IF NOT EXISTS library_user (
    user_id serial primary key,
    first_name text not null,
    last_name text not null,
    gender gender_domain not null,
    email email_domain not null,
    birthday timestamp not null,
    added_at timestamp not null default now()
);

CREATE INDEX idx_library_user ON pfp (user_id);

CREATE TABLE IF NOT EXISTS pfp (
    user_id int primary key references library_user(user_id) on delete cascade,
    pfp_url text not null,
    added_at timestamp not null default now()
);

CREATE TABLE IF NOT EXISTS author (
    author_id serial primary key,
    full_name text not null,
    added_at timestamp not null default now()
) CREATE TABLE IF NOT EXISTS publisher (
    publisher_id serial primary key,
    publisher_name text not null
) CREATE INDEX idx_publisher ON work (publisher_id);

CREATE TABLE IF NOT EXISTS work (
    work_id serial primary key,
    publisher_id int not null references publisher(publisher_id) on delete cascade,
    title text not null,
    pages int not null check (pages >= 0),
    release_year int not null,
    added_at timestamp not null default now()
);

CREATE TRIGGER check_year BEFORE
INSERT
    OR
UPDATE
    ON work FOR EACH ROW EXECUTE PROCEDURE check_release_year();

CREATE TABLE IF NOT EXISTS work_author (
    work_id int references work(work_id) on delete cascade,
    author_id int references author(author_id) on delete cascade,
    primary key(work_id, author_id)
);

CREATE INDEX idx_work_author_work ON work_author (work_id);

CREATE INDEX idx_work_author_author ON work_author (author_id);

CREATE TABLE IF NOT EXISTS work_weight (
    work_id int primary key references work(work_id) on delete cascade,
    weight float not null check (weight >= 0)
);

CREATE INDEX idx_work_weight ON work_weight (work_id);

CREATE TABLE IF NOT EXISTS work_isbn (
    work_id int primary key references work(work_id) on delete cascade,
    isbn varchar(13) not null check (length(isbn) = 13)
);

CREATE INDEX idx_work_isbn ON work_isbn (work_id);

CREATE INDEX idx_work_isbn_isbn ON work_isbn (isbn);

CREATE TABLE IF NOT EXISTS lang (
    language_id varchar(3) primary key check (char_length(language_id) = 3),
    lang_name text not null,
    speakers bigint not null check (speakers >= 0),
    added_at timestamp not null default now()
);

CREATE TABLE IF NOT EXISTS work_language (
    work_id int primary key references work(work_id) on delete cascade,
    language_id varchar(3) references lang(language_id) on delete cascade
);

CREATE INDEX idx_work_language_work ON work_language (work_id);

CREATE INDEX idx_work_language_lang ON work_language (language_id);

CREATE TABLE IF NOT EXISTS subject (
    subject_id serial primary key,
    subject_name text not null
);

CREATE TABLE IF NOT EXISTS work_subject(
    work_id int references work(work_id) on delete cascade,
    subject_id int references subject(subject_id) on delete cascade,
    primary key(work_id, subject_id)
);

CREATE INDEX idx_work_subject_work ON work_subject (work_id);

CREATE INDEX idx_work_subject_subject ON work_subject (subject_id);

CREATE TABLE IF NOT EXISTS rating (
    rating_id serial primary key,
    user_id int references library_user(user_id) on delete cascade,
    work_id int references work(work_id) on delete cascade,
    score int not null check (
        0 < score
        and score <= 5
    ),
    rated_at timestamp not null default now()
);

CREATE INDEX idx_rating_user ON rating (user_id);

CREATE INDEX idx_rating_work ON rating (work_id);

CREATE TABLE IF NOT EXISTS listing (
    listing_id serial primary key,
    user_id int references library_user(user_id) on delete cascade,
    work_id int references work(work_id) on delete cascade,
    reading_status reading_status_type not null,
    listed_at timestamp not null default now()
);

CREATE INDEX idx_listing_user ON listing (user_id);

CREATE INDEX idx_listing_work ON listing (work_id);

CREATE TABLE IF NOT EXISTS inventory_item(
    item_id serial primary key,
    work_id int references work(work_id),
    medium item_medium_type not null,
    added_at timestamp not null default now()
);

CREATE INDEX idx_inventory_item_work ON inventory_item (work_id);

CREATE TABLE IF NOT EXISTS loan(
    loan_id serial primary key,
    user_id int references library_user(user_id) on delete cascade,
    item_id int references inventory_item(item_id) on delete cascade,
    loaned_at timestamp not null default now()
);

CREATE INDEX idx_loan_user ON loan (user_id);

CREATE INDEX idx_loan_item ON loan (item_id);

CREATE TABLE IF NOT EXISTS loan_return(
    loan_id int primary key references loan(loan_id),
    returned_at timestamp not null default now()
);

CREATE INDEX idx_loan_return ON loan_return (loan_id);
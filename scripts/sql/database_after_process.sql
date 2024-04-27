-- Move medium column from inventory_item to work

ALTER TABLE work ADD COLUMN medium item_medium_type default 'BOOK' not null;

UPDATE work 
SET medium = COALESCE(inventory_item.medium, 'BOOK')
FROM inventory_item
WHERE work.work_id = inventory_item.work_id 
AND inventory_item.medium != 'BOOK';

alter table inventory_item drop column medium;

-- Create funcions and set triggers
CREATE OR REPLACE FUNCTION check_release_year() RETURNS TRIGGER AS $$
BEGIN
    IF NEW.release_year > EXTRACT(YEAR FROM NOW()) THEN
        RAISE EXCEPTION 'Release year cannot be in the future';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER check_release_year_year BEFORE
INSERT OR UPDATE
ON work FOR EACH ROW EXECUTE PROCEDURE CHECK_release_year();

CREATE OR REPLACE FUNCTION update_modified_at() RETURNS TRIGGER AS $$
BEGIN
   NEW.modified_at = NOW();
   RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_author_modified_at_trigger
BEFORE UPDATE ON lang
FOR EACH ROW EXECUTE PROCEDURE update_modified_at();
CREATE TRIGGER update_author_modified_at_trigger
BEFORE UPDATE ON subject
FOR EACH ROW EXECUTE PROCEDURE update_modified_at();
CREATE TRIGGER update_author_modified_at_trigger
BEFORE UPDATE ON publisher
FOR EACH ROW EXECUTE PROCEDURE update_modified_at();
CREATE TRIGGER update_author_modified_at_trigger
BEFORE UPDATE ON library_user
FOR EACH ROW EXECUTE PROCEDURE update_modified_at();
CREATE TRIGGER update_author_modified_at_trigger
BEFORE UPDATE ON author
FOR EACH ROW EXECUTE PROCEDURE update_modified_at();
CREATE TRIGGER update_author_modified_at_trigger
BEFORE UPDATE ON work
FOR EACH ROW EXECUTE PROCEDURE update_modified_at();

-- Create indices
CREATE INDEX idx_library_user ON pfp (user_id);
CREATE INDEX idx_author_full_name ON author USING gin(full_name gin_trgm_ops);
CREATE INDEX idx_publisher_name ON publisher USING gin(publisher_name gin_trgm_ops);
CREATE INDEX idx_language_name ON lang USING gin(lang_name gin_trgm_ops);
CREATE INDEX idx_work_publisher ON work (publisher_id);
CREATE INDEX idx_work_language ON work(language_id);
CREATE INDEX idx_isbn ON work USING gin(isbn gin_trgm_ops);
CREATE INDEX idx_title ON work USING gin(title gin_trgm_ops);
CREATE INDEX idx_work_author_work ON work_author (work_id);
CREATE INDEX idx_work_author_author ON work_author (author_id);
CREATE INDEX idx_work_author_author_work ON work_author (author_id, work_id);
CREATE INDEX idx_subject_name ON subject USING gin(subject_name gin_trgm_ops);
CREATE INDEX idx_work_subject_work ON work_subject (work_id);
CREATE INDEX idx_work_subject_subject ON work_subject (subject_id);
CREATE INDEX idx_rating_user ON rating (user_id);
CREATE INDEX idx_rating_work ON rating (work_id);
CREATE INDEX idx_listing_user ON listing (user_id);
CREATE INDEX idx_listing_work ON listing (work_id);
CREATE INDEX idx_reading_status ON listing (reading_status);
CREATE INDEX idx_inventory_item_work ON inventory_item (work_id);
CREATE INDEX idx_loan_user ON loan (user_id);
CREATE INDEX idx_loan_item ON loan (item_id);
CREATE INDEX idx_loan_return ON loan_return (loan_id);
CREATE INDEX idx_work_subject_work_id ON work_subject(work_id);
CREATE INDEX idx_work_id ON work(work_id);

-- Add constraints
ALTER TABLE lang ADD CONSTRAINT lang_id_length_check CHECK (CHAR_LENGTH(language_id) = 3);
ALTER TABLE lang ADD CONSTRAINT speakers_non_negative_check CHECK (speakers >= 0);
ALTER TABLE work ADD CONSTRAINT isbn_length_check CHECK (LENGTH(isbn) = 13);
ALTER TABLE work ADD CONSTRAINT pages_non_negative_check CHECK (pages >= 0);
ALTER TABLE work ADD CONSTRAINT weight_non_negative_check CHECK (weight >= 0);
ALTER TABLE rating ADD CONSTRAINT score_range_check CHECK (0 < score AND score <= 5);

-- Set serial primary keys valus
SELECT setval('library_user_user_id_seq', (SELECT MAX(user_id) FROM library_user));
SELECT setval('author_author_id_seq', (SELECT MAX(author_id) FROM author));
SELECT setval('publisher_publisher_id_seq', (SELECT MAX(publisher_id) FROM publisher));
SELECT setval('work_work_id_seq', (SELECT MAX(work_id) FROM work));
SELECT setval('subject_subject_id_seq', (SELECT MAX(subject_id) FROM subject));
SELECT setval('rating_rating_id_seq', (SELECT MAX(rating_id) FROM rating));
SELECT setval('listing_listing_id_seq', (SELECT MAX(listing_id) FROM listing));
SELECT setval('inventory_item_item_id_seq', (SELECT MAX(item_id) FROM inventory_item));
SELECT setval('loan_loan_id_seq', (SELECT MAX(loan_id) FROM loan));

-- Add admin user
WITH new_user AS (
  INSERT INTO library_user(first_name, last_name, gender, email, birthday, role, modified_at) 
  VALUES ('Maksym','Myna','m','maksymkomyna@gmail.com','1888-01-22','ADMIN', '2024-04-12 04:41:26.391')
  RETURNING user_id
)
INSERT INTO pfp(user_id, pfp_url) 
SELECT user_id, 'https://lh3.googleusercontent.com/a/ACg8ocIkQuLkcTAvp_LoVBrAvBEPD92fwTSamT7JjcW5DHTrfo5fWRt1=s500-c' 
FROM new_user;

-- Ensure data necessary for UI got inserted
WITH neal_schuman_publishing AS (
  INSERT INTO publisher(publisher_name) 
  VALUES ('Neal Schuman Publishing')
  ON CONFLICT (publisher_name) DO UPDATE SET publisher_name = EXCLUDED.publisher_name
  RETURNING publisher_id
)
INSERT INTO work(publisher_id, isbn, language_id, title, pages, weight, release_year, medium) 
SELECT neal_schuman_publishing.publisher_id, '9780375712364', 'eng', 'Brave New World', 469, 549.0, 2013, 'BOOK'
FROM neal_schuman_publishing
ON CONFLICT (isbn) DO NOTHING;

WITH aldous_huxley AS (
  insert into author(full_name) values ('Aldous Huxley')
  ON CONFLICT (full_name) DO UPDATE SET full_name = EXCLUDED.full_name
  returning author_id
)
insert into work_author(author_id, work_id) 
select aldous_huxley.author_id, work_id from work, aldous_huxley where isbn in (
  '9780375712364',
  '9798468471463',
  '9780006547280',
  '9780803946484',
  '9798715985033',
  '9780060595180',
  '9780006547464',
  '9780060120917'
)
ON CONFLICT (work_id, author_id) DO NOTHING;



CREATE OR REPLACE PROCEDURE insert_work_and_subject(
  _publisher_name TEXT,
  _isbn TEXT,
  _language_id TEXT,
  _title TEXT,
  _pages INT,
  _weight NUMERIC,
  _release_year INT,
  _medium TEXT,
  _subject_id INT,
  _author_name TEXT
)
LANGUAGE plpgsql
AS $$
BEGIN
  WITH publisher AS (
    INSERT INTO publisher(publisher_name) 
    VALUES (_publisher_name)
    ON CONFLICT (publisher_name) DO UPDATE SET publisher_name = EXCLUDED.publisher_name
    RETURNING publisher_id
  ), work AS (
    INSERT INTO work(publisher_id, isbn, language_id, title, pages, weight, release_year, medium) 
    SELECT publisher.publisher_id, _isbn, _language_id, _title, _pages, _weight, _release_year, _medium::item_medium_type
    FROM publisher
    ON CONFLICT (isbn) DO UPDATE SET title = EXCLUDED.title
    RETURNING work_id
  ), author AS (
    INSERT INTO author(full_name) 
    VALUES (_author_name)
    ON CONFLICT (full_name) DO UPDATE SET full_name = EXCLUDED.full_name
    RETURNING author_id
  ), work_author AS (
    INSERT INTO work_author(work_id, author_id) 
    SELECT work.work_id, author.author_id
    FROM work, author
    ON CONFLICT (work_id, author_id) DO NOTHING
  )
  INSERT INTO work_subject(work_id, subject_id) 
  SELECT work.work_id, _subject_id
  FROM work
  ON CONFLICT (work_id, subject_id) DO NOTHING;
END;
$$;

CALL insert_work_and_subject('Little, Brown Book Group Limited', '9780349113463', 'eng', 'Tipping Point: How Little Things Can Make a Big Difference', 188, 0.72, 2002, 'BOOK', 0, 'Malcolm Gladwell');
CALL insert_work_and_subject('Mira', '9781551664798', 'eng', 'Duncans Bride', 138, 0.14, 1998, 'BOOK', 1, 'Linda Howard');
CALL insert_work_and_subject('Holt Rinehart And Winston', '9780030018893', 'eng', 'B is for burglar: Sue Grafton', 229, 0.57, 1985, 'BOOK', 2, 'Sue Grafton');
CALL insert_work_and_subject('Unknown', '9783596706648', 'eng', 'Ready Player One', 181, 2.036, 1952, 'BOOK', 4, 'Ernest Cline');
CALL insert_work_and_subject('Harlequin', '9780373031122', 'eng', 'An impossible passion: Stephanie Howard', 187, 1.34, 1991, 'BOOK', 5, 'Stephanie Howard');
CALL insert_work_and_subject('Megan Tingley Books/Little, Brown And Company', '9780316015844', 'eng', 'Twilight', 177, 1.4, 2006, 'BOOK', 6, 'Stephanie Meyer');
CALL insert_work_and_subject('Unknown', '9780349429793', 'eng', 'The Viscount Who Loved Me: Inspiration for the Netflix Original Series Bridgerton', 146, 2.457, 1940, 'BOOK', 7, 'Juliq Quinn');
CALL insert_work_and_subject('Simon Schuster', '9781797149493', 'eng', 'A Man Called Ove: A Novel', 297, 1.4, 2022, 'BOOK', 8, 'Fredrick Backman');
CALL insert_work_and_subject('Faber', '9780571191475', 'eng', 'Lord of the flies: William Golding', 225, 2.56, 2002, 'BOOK', 9, 'William Golding');
CALL insert_work_and_subject('Marvel Worldwide', '9781302950132', 'eng', 'Avengers by Jason Aaron Vol. 4', 352, 0.457, 2023, 'BOOK', 10, 'Jason Aaron');
CALL insert_work_and_subject('Caedmon', '9780061650499', 'eng', 'A Tree Grows in Brooklyn Low Price CD', 379, 2.95, 2008, 'BOOK', 11, 'Betty Smith');
CALL insert_work_and_subject('Rentice Hall', '9780131962941', 'eng', 'Classical Myth 5th Edition', 752, 1.8, 2006, 'BOOK', 12, 'Barry B. Powell');
CALL insert_work_and_subject('Clarkson Potter', '9780307381378', 'eng', 'Georgia cooking in an Oklahoma kitchen: recipes from my family to yours Trisha Yearwood, with Gwen Yearwood and Beth Berman', 235, 2, 2008, 'BOOK', 13, 'Trisha Yearwood');
CALL insert_work_and_subject('Clarendon', '9780198593782', 'eng', 'Electrical machines and drives: a space-vector theory approach Peter Vas', 808, 0.8, 1992, 'BOOK', 14 , 'Peter Vas');
CALL insert_work_and_subject('Rfection Learning', '9781606860380', 'eng', 'The Sea of Monsters Percy Jackson and the Olympians, Book 2', 279, 0.47, 2003, 'BOOK', 15, 'Rick Riordan');
CALL insert_work_and_subject('Indypublish', '9781435327870', 'eng', 'St Ives', 260, 0.54, 2007, 'BOOK', 16, 'Louis Robert Stevenson');
CALL insert_work_and_subject('Unknown', '9781574531725', 'eng', 'Phoenix Rising: No-Eyes Vision of the Change to Come', 203, 0.36, 1997, 'BOOK', 17, 'Nancy Fish');
CALL insert_work_and_subject('Christian Herald Books', '9780915684687', 'eng', 'Bible Stories to Grow by', 202, 2.51, 1980, 'BOOK', 18, 'Mary Batchelor');
CALL insert_work_and_subject('Hodder', '9780340918616', 'eng', 'My sisters keeper: Jodi Picoult', 364, 0.91, 2008, 'BOOK', 19, 'Jodi Picoult');
CALL insert_work_and_subject('Rentice Hall', '9780130998170', 'eng', 'Macroeconomics: Theories and Policies 6th Edition', 481, 1.2, 1998, 'BOOK', 20, 'Richar T. Froyen');


--- Creating materialized views
CREATE MATERIALIZED VIEW
  available_copies AS
SELECT
  work_id,
  COUNT(ii) AS qty
FROM
  inventory_item ii
LEFT JOIN
  loan l
USING
  (item_id)
LEFT JOIN
  loan_return lr
USING
  (loan_id)
WHERE
  (l.loaned_at = (
    SELECT
      MAX(l2.loaned_at)
    FROM
      loan l2
    LEFT JOIN
      loan_return lr2
    ON
      l2.loan_id = lr2.loan_id
    WHERE
      l2.item_id = l.item_id )
    AND lr.returned_at IS NOT NULL)
  OR l.loaned_At IS NULL
GROUP BY
  work_id;

CREATE MATERIALIZED VIEW
  publisher_work_count AS
SELECT
  publisher_name,
  COUNT(*) AS work_count
FROM
  publisher
JOIN
  work
USING
  (publisher_id)
GROUP BY
  publisher_name
ORDER BY
  work_count DESC;

CREATE MATERIALIZED VIEW
  author_work_count AS
SELECT
  full_name,
  COUNT(*) AS work_count
FROM
  author
JOIN
  work_author
USING
  (author_id)
GROUP BY
  full_name
ORDER BY
  work_count DESC;

CREATE INDEX idx_author_work_counts_full_name ON author_work_count(full_name);  
CREATE INDEX idx_publisher_work_counts_publisher_name ON publisher_work_count(publisher_name);
CREATE INDEX idx_available_copies_work_id ON available_copies(work_id);

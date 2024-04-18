ALTER TABLE work ADD COLUMN medium item_medium_type default 'BOOK' not null;

UPDATE work 
SET medium = COALESCE(inventory_item.medium, 'BOOK')
FROM inventory_item
WHERE work.work_id = inventory_item.work_id 
AND inventory_item.medium != 'BOOK';

alter table inventory_item drop column medium;

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

-- CREATE TRIGGER update_item_modified_at_trigger
-- BEFORE UPDATE ON inventory_item
-- FOR EACH ROW EXECUTE PROCEDURE update_modified_at();
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

CREATE INDEX idx_library_user ON pfp (user_id);
CREATE INDEX idx_author_full_name ON author USING gin(full_name gin_trgm_ops);
CREATE INDEX idx_publisher_name ON publisher USING gin(publisher_name gin_trgm_ops);
CREATE INDEX idx_work_publisher ON work (publisher_id);
CREATE INDEX idx_work_language ON work(language_id);
CREATE INDEX idx_isbn ON work USING gin(isbn gin_trgm_ops);
CREATE INDEX idx_title ON work USING gin(title gin_trgm_ops);
CREATE INDEX idx_work_author_work ON work_author (work_id);
CREATE INDEX idx_work_author_author ON work_author (author_id);
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

ALTER TABLE lang ADD CONSTRAINT lang_id_length_check CHECK (CHAR_LENGTH(language_id) = 3);
ALTER TABLE lang ADD CONSTRAINT speakers_non_negative_check CHECK (speakers >= 0);
ALTER TABLE work ADD CONSTRAINT isbn_length_check CHECK (LENGTH(isbn) = 13);
ALTER TABLE work ADD CONSTRAINT pages_non_negative_check CHECK (pages >= 0);
ALTER TABLE work ADD CONSTRAINT weight_non_negative_check CHECK (weight >= 0);
ALTER TABLE rating ADD CONSTRAINT score_range_check CHECK (0 < score AND score <= 5);

SELECT setval('library_user_user_id_seq', (SELECT MAX(user_id) FROM library_user));
SELECT setval('author_author_id_seq', (SELECT MAX(author_id) FROM author));
SELECT setval('publisher_publisher_id_seq', (SELECT MAX(publisher_id) FROM publisher));
SELECT setval('work_work_id_seq', (SELECT MAX(work_id) FROM work));
SELECT setval('subject_subject_id_seq', (SELECT MAX(subject_id) FROM subject));
SELECT setval('rating_rating_id_seq', (SELECT MAX(rating_id) FROM rating));
SELECT setval('listing_listing_id_seq', (SELECT MAX(listing_id) FROM listing));
SELECT setval('inventory_item_item_id_seq', (SELECT MAX(item_id) FROM inventory_item));
SELECT setval('loan_loan_id_seq', (SELECT MAX(loan_id) FROM loan));

WITH new_user AS (
  INSERT INTO library_user(first_name, last_name, gender, email, birthday, role, modified_at) 
  VALUES ('Maksym','Myna','m','maksymkomyna@gmail.com','1888-01-22','ADMIN', '2024-04-12 04:41:26.391')
  RETURNING user_id
)
INSERT INTO pfp(user_id, pfp_url) 
SELECT user_id, 'https://lh3.googleusercontent.com/a/ACg8ocIkQuLkcTAvp_LoVBrAvBEPD92fwTSamT7JjcW5DHTrfo5fWRt1=s500-c' 
FROM new_user;
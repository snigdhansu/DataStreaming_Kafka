CREATE TABLE trending_repositories (
    id SERIAL PRIMARY KEY,               -- Auto-incrementing unique identifier
    repo_name VARCHAR(255) NOT NULL,     -- Name of the GitHub repository
    action_count INT NOT NULL         -- Number of stars gained in the window
);

-- CREATE OR REPLACE FUNCTION maintain_top_10_records()
-- RETURNS TRIGGER AS $$
-- BEGIN
--     -- Check if the number of records exceeds 10
--     IF (SELECT COUNT(*) FROM trending_repositories) > 10 THEN
--         -- Delete the record with the smallest action_count, assuming this is the "top" record to remove
--         DELETE FROM trending_repositories
--         WHERE repo_name IN (
--             SELECT repo_name
--             FROM trending_repositories
--             ORDER BY action_count DESC -- Adjust order to match the "top" logic you need
--             LIMIT 1 OFFSET 10 -- Deletes the 11th record (the one to remove)
--         );
--     END IF;
--     RETURN NEW; -- Proceed with the insert
-- END;
-- $$ LANGUAGE plpgsql;

-- CREATE TRIGGER enforce_top_10_records
-- AFTER INSERT ON trending_repositories
-- FOR EACH ROW
-- EXECUTE FUNCTION maintain_top_10_records();
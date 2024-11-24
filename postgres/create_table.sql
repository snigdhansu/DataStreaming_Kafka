CREATE TABLE trending_repositories (
    id SERIAL PRIMARY KEY,               -- Auto-incrementing unique identifier
    repo_name VARCHAR(255) NOT NULL,     -- Name of the GitHub repository
    action_count INT NOT NULL         -- Number of stars gained in the window
);
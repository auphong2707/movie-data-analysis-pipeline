-- Movie data cleaning transformation
-- Removes invalid records and standardizes formats

WITH cleaned_movies AS (
  SELECT 
    id,
    TRIM(title) as title,
    TRIM(overview) as overview,
    CASE 
      WHEN release_date = '' OR release_date IS NULL THEN NULL
      ELSE CAST(release_date AS DATE)
    END as release_date,
    poster_path,
    backdrop_path,
    CASE 
      WHEN popularity < 0 THEN 0
      WHEN popularity > 1000 THEN 1000
      ELSE popularity
    END as popularity,
    CASE 
      WHEN vote_average < 0 THEN 0
      WHEN vote_average > 10 THEN 10
      ELSE vote_average
    END as vote_average,
    CASE 
      WHEN vote_count < 0 THEN 0
      ELSE vote_count
    END as vote_count,
    adult,
    UPPER(original_language) as original_language,
    TRIM(original_title) as original_title,
    genre_ids,
    video,
    CURRENT_TIMESTAMP as processed_at
  FROM {{ source('tmdb', 'popular_movies') }}
  WHERE 
    id IS NOT NULL 
    AND title IS NOT NULL 
    AND TRIM(title) != ''
    AND id > 0
)

SELECT * FROM cleaned_movies
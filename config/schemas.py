"""
Avro schemas for movie data pipeline.
"""

# Movie schema
MOVIE_SCHEMA = """
{
  "type": "record",
  "name": "Movie",
  "namespace": "movie.analytics",
  "fields": [
    {"name": "id", "type": "int"},
    {"name": "title", "type": "string"},
    {"name": "original_title", "type": ["null", "string"], "default": null},
    {"name": "overview", "type": ["null", "string"], "default": null},
    {"name": "release_date", "type": ["null", "string"], "default": null},
    {"name": "adult", "type": "boolean", "default": false},
    {"name": "popularity", "type": ["null", "double"], "default": null},
    {"name": "vote_average", "type": ["null", "double"], "default": null},
    {"name": "vote_count", "type": ["null", "int"], "default": null},
    {"name": "poster_path", "type": ["null", "string"], "default": null},
    {"name": "backdrop_path", "type": ["null", "string"], "default": null},
    {"name": "original_language", "type": ["null", "string"], "default": null},
    {"name": "budget", "type": ["null", "long"], "default": null},
    {"name": "revenue", "type": ["null", "long"], "default": null},
    {"name": "runtime", "type": ["null", "int"], "default": null},
    {"name": "status", "type": ["null", "string"], "default": null},
    {"name": "tagline", "type": ["null", "string"], "default": null},
    {"name": "homepage", "type": ["null", "string"], "default": null},
    {"name": "imdb_id", "type": ["null", "string"], "default": null},
    {
      "name": "genres",
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "Genre",
          "fields": [
            {"name": "id", "type": "int"},
            {"name": "name", "type": "string"}
          ]
        }
      },
      "default": []
    },
    {
      "name": "production_companies",
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "ProductionCompany",
          "fields": [
            {"name": "id", "type": "int"},
            {"name": "name", "type": "string"},
            {"name": "logo_path", "type": ["null", "string"], "default": null},
            {"name": "origin_country", "type": ["null", "string"], "default": null}
          ]
        }
      },
      "default": []
    },
    {"name": "ingestion_timestamp", "type": "double"},
    {"name": "data_source", "type": "string", "default": "tmdb"}
  ]
}
"""

# Person schema
PERSON_SCHEMA = """
{
  "type": "record",
  "name": "Person",
  "namespace": "movie.analytics",
  "fields": [
    {"name": "id", "type": "int"},
    {"name": "name", "type": "string"},
    {"name": "biography", "type": ["null", "string"], "default": null},
    {"name": "birthday", "type": ["null", "string"], "default": null},
    {"name": "deathday", "type": ["null", "string"], "default": null},
    {"name": "place_of_birth", "type": ["null", "string"], "default": null},
    {"name": "profile_path", "type": ["null", "string"], "default": null},
    {"name": "adult", "type": "boolean", "default": false},
    {"name": "popularity", "type": ["null", "double"], "default": null},
    {"name": "gender", "type": ["null", "int"], "default": null},
    {"name": "known_for_department", "type": ["null", "string"], "default": null},
    {"name": "homepage", "type": ["null", "string"], "default": null},
    {"name": "imdb_id", "type": ["null", "string"], "default": null},
    {
      "name": "also_known_as",
      "type": {
        "type": "array",
        "items": "string"
      },
      "default": []
    },
    {"name": "ingestion_timestamp", "type": "double"},
    {"name": "data_source", "type": "string", "default": "tmdb"}
  ]
}
"""

# Credit schema
CREDIT_SCHEMA = """
{
  "type": "record",
  "name": "Credit",
  "namespace": "movie.analytics",
  "fields": [
    {"name": "movie_id", "type": "int"},
    {"name": "person_id", "type": "int"},
    {"name": "credit_type", "type": {"type": "enum", "name": "CreditType", "symbols": ["cast", "crew"]}},
    {"name": "character", "type": ["null", "string"], "default": null},
    {"name": "job", "type": ["null", "string"], "default": null},
    {"name": "department", "type": ["null", "string"], "default": null},
    {"name": "order", "type": ["null", "int"], "default": null},
    {"name": "name", "type": "string"},
    {"name": "profile_path", "type": ["null", "string"], "default": null},
    {"name": "gender", "type": ["null", "int"], "default": null},
    {"name": "adult", "type": "boolean", "default": false},
    {"name": "known_for_department", "type": ["null", "string"], "default": null},
    {"name": "popularity", "type": ["null", "double"], "default": null},
    {"name": "ingestion_timestamp", "type": "double"},
    {"name": "data_source", "type": "string", "default": "tmdb"}
  ]
}
"""

# Review schema
REVIEW_SCHEMA = """
{
  "type": "record",
  "name": "Review",
  "namespace": "movie.analytics",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "movie_id", "type": "int"},
    {"name": "author", "type": "string"},
    {"name": "content", "type": "string"},
    {"name": "created_at", "type": ["null", "string"], "default": null},
    {"name": "updated_at", "type": ["null", "string"], "default": null},
    {"name": "url", "type": ["null", "string"], "default": null},
    {
      "name": "author_details",
      "type": {
        "type": "record",
        "name": "AuthorDetails",
        "fields": [
          {"name": "name", "type": ["null", "string"], "default": null},
          {"name": "username", "type": ["null", "string"], "default": null},
          {"name": "avatar_path", "type": ["null", "string"], "default": null},
          {"name": "rating", "type": ["null", "double"], "default": null}
        ]
      },
      "default": null
    },
    {"name": "ingestion_timestamp", "type": "double"},
    {"name": "data_source", "type": "string", "default": "tmdb"}
  ]
}
"""

# Rating schema
RATING_SCHEMA = """
{
  "type": "record",
  "name": "Rating",
  "namespace": "movie.analytics",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "movie_id", "type": "int"},
    {"name": "user_id", "type": ["null", "string"], "default": null},
    {"name": "rating", "type": "double"},
    {"name": "timestamp", "type": "double"},
    {"name": "source", "type": "string", "default": "tmdb"},
    {"name": "ingestion_timestamp", "type": "double"},
    {"name": "data_source", "type": "string", "default": "tmdb"}
  ]
}
"""

# Schema registry mapping
SCHEMAS = {
    'movies': MOVIE_SCHEMA,
    'people': PERSON_SCHEMA,
    'credits': CREDIT_SCHEMA,
    'reviews': REVIEW_SCHEMA,
    'ratings': RATING_SCHEMA
}
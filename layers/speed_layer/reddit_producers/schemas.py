"""
Avro Schemas for Reddit Stream Data

Defines schemas for reddit.posts and reddit.comments Kafka topics.
"""

REDDIT_POST_SCHEMA = {
    "type": "record",
    "name": "RedditPost",
    "namespace": "com.movieanalytics.speed",
    "fields": [
        {"name": "post_id", "type": "string", "doc": "Reddit post ID (unique)"},
        {"name": "subreddit", "type": "string", "doc": "Subreddit name"},
        {"name": "title", "type": "string", "doc": "Post title"},
        {"name": "selftext", "type": ["null", "string"], "default": None, "doc": "Post body text"},
        {"name": "author", "type": "string", "doc": "Reddit username or [deleted]"},
        {"name": "created_utc", "type": "double", "doc": "UTC timestamp when posted"},
        {"name": "upvotes", "type": "int", "doc": "Score (upvotes - downvotes)"},
        {"name": "upvote_ratio", "type": "float", "doc": "Percentage of upvotes (0.0-1.0)"},
        {"name": "num_comments", "type": "int", "doc": "Number of comments"},
        {"name": "awards", "type": "int", "doc": "Total awards received"},
        {"name": "url", "type": "string", "doc": "Post URL"},
        {"name": "is_self", "type": "boolean", "doc": "Is text post (not link)"},
        {
            "name": "potential_movies",
            "type": {"type": "array", "items": "string"},
            "doc": "Extracted potential movie titles"
        },
        {"name": "fetched_at", "type": "string", "doc": "ISO 8601 timestamp when fetched"}
    ]
}

REDDIT_COMMENT_SCHEMA = {
    "type": "record",
    "name": "RedditComment",
    "namespace": "com.movieanalytics.speed",
    "fields": [
        {"name": "comment_id", "type": "string", "doc": "Reddit comment ID (unique)"},
        {"name": "post_id", "type": "string", "doc": "Parent post ID"},
        {"name": "subreddit", "type": "string", "doc": "Subreddit name"},
        {"name": "body", "type": "string", "doc": "Comment text"},
        {"name": "author", "type": "string", "doc": "Reddit username or [deleted]"},
        {"name": "created_utc", "type": "double", "doc": "UTC timestamp when posted"},
        {"name": "upvotes", "type": "int", "doc": "Score (upvotes - downvotes)"},
        {"name": "awards", "type": "int", "doc": "Total awards received"},
        {"name": "parent_id", "type": "string", "doc": "Parent comment/post ID"},
        {"name": "is_submitter", "type": "boolean", "doc": "Is comment author the post author"},
        {
            "name": "potential_movies",
            "type": {"type": "array", "items": "string"},
            "doc": "Extracted potential movie titles"
        },
        {"name": "fetched_at", "type": "string", "doc": "ISO 8601 timestamp when fetched"}
    ]
}

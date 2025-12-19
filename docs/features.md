# API Feature Mapping for Business Goals

This document maps available API features from **Reddit API** and **TMDB API** to the three business goals defined in the project.

---

## Business Goal #1: PR Crisis Detection & Sentiment Monitoring

**Goal**: Detect when current Reddit discussion sentiment drops significantly below historical TMDB baselines

### Reddit API (Speed Layer) - Available Features

**Source**: https://www.reddit.com/dev/api/

| **Endpoint** | **Data Fields** | **Supports Task** |
|-------------|----------------|-------------------|
| `GET /r/{subreddit}/new` | • `title` (post title) **[Reddit]**<br>• `selftext` (post body text) **[Reddit]**<br>• `score` (upvotes - downvotes) **[Reddit]**<br>• `ups` (upvote count) **[Reddit]**<br>• `downs` (downvote count) **[Reddit]**<br>• `created_utc` (timestamp) **[Reddit]**<br>• `num_comments` (comment count) **[Reddit]**<br>• `author` (username) **[Reddit]**<br>• `subreddit` (subreddit name) **[Reddit]** | ✅ Real-time post retrieval for sentiment analysis |
| `GET /r/{subreddit}/comments/{article}` | • `body` (comment text) **[Reddit]**<br>• `score` (comment score) **[Reddit]**<br>• `ups` / `downs` **[Reddit]**<br>• `created_utc` **[Reddit]**<br>• `author` **[Reddit]**<br>• `depth` (comment nesting level) **[Reddit]** | ✅ Comment text for sentiment analysis |
| `POST /api/vote` | • `id` (post/comment ID) **[Reddit]**<br>• `dir` (vote direction: 1, 0, -1) **[Reddit]** | ❌ Not needed (read-only access) |
| `GET /r/{subreddit}/hot` | • All fields from `/new` **[Reddit]**<br>• Sorted by Reddit's "hot" algorithm | ✅ Trending discussions for crisis detection |
| `GET /r/{subreddit}/rising` | • All fields from `/new` **[Reddit]**<br>• Early-stage trending content | ✅ Emerging negative sentiment detection |

**Key Features for Sentiment Monitoring:**
- ✅ **Text Content**: `title`, `selftext`, `body` **[Reddit]** → VADER sentiment analysis input
- ✅ **Engagement Signals**: `score`, `ups`, `downs`, `num_comments` **[Reddit]** → Community reaction intensity
- ✅ **Temporal Data**: `created_utc` **[Reddit]** → Track sentiment velocity (6-hour drops)
- ✅ **Discussion Context**: `subreddit`, `depth` **[Reddit]** → Cross-subreddit sentiment comparison

**Rate Limit**: 60 requests/minute (free tier)

---

### TMDB API (Batch Layer) - Available Features

**Source**: https://developer.themoviedb.org/reference/

| **Endpoint** | **Data Fields** | **Supports Task** |
|-------------|----------------|-------------------|
| `GET /movie/{movie_id}` | • `title` **[TMDB]**<br>• `overview` (movie description) **[TMDB]**<br>• `vote_average` (average rating) **[TMDB]**<br>• `vote_count` (number of votes) **[TMDB]**<br>• `popularity` (TMDB popularity score) **[TMDB]**<br>• `release_date` **[TMDB]**<br>• `genres` (array of genre IDs) **[TMDB]**<br>• `runtime` **[TMDB]**<br>• `budget`, `revenue` **[TMDB]** | ✅ Movie metadata for context |
| `GET /movie/{movie_id}/changes` | • `changes` array with field-level updates **[TMDB]**<br>• Tracks vote_count, review additions, etc. | ❌ Not useful for historical baselines |
| `GET /movie/popular` | • List of popular movies with basic metadata **[TMDB]** | ✅ Identify comparable movies for baseline calculation |

**Key Features for Historical Baselines:**
- ✅ **Review Archive**: `reviews.content` **[TMDB]** → Historical sentiment analysis (5 years of data)
- ✅ **Genre Context**: `genres` **[TMDB]** + `genre/movie/list` **[TMDB]** → Genre-specific baselines (e.g., sci-fi avg: +0.65)
- ✅ **Statistical Data**: `vote_average`, `vote_count` **[TMDB]** → Calculate sentiment variance (±0.15 normal range)
- ✅ **Temporal Context**: `reviews.created_at` **[TMDB]** → Identify franchise patterns (e.g., Dune 1 vs Dune 2)
- ✅ **Popularity Metrics**: `popularity` **[TMDB]** → Filter for comparable releasesent variance (±0.15 normal range)
- ✅ **Temporal Context**: `reviews.created_at` → Identify franchise patterns (e.g., Dune 1 vs Dune 2)
- ✅ **Popularity Metrics**: `popularity` → Filter for comparable releases

**Rate Limit**: 4 requests/second

---

| **Task** | **Reddit Features Used** | **TMDB Features Used** | **Status** |
|---------|-------------------------|------------------------|-----------|
| **Detect sentiment drops** | `body` **[Reddit]**, `selftext` **[Reddit]** (VADER input)<br>`created_utc` **[Reddit]** (velocity tracking) | `reviews.content` **[TMDB]** (baseline sentiment)<br>`genres` **[TMDB]** (genre avg) | ✅ Fully Supported |
| **Calculate baselines** | N/A | `reviews.content` **[TMDB]** + `genres` **[TMDB]**<br>`vote_average` **[TMDB]** (statistical thresholds) | ✅ Fully Supported |
| **Track sentiment velocity** | `score` **[Reddit]**, `created_utc` **[Reddit]** (6-hour windows) | N/A | ✅ Fully Supported |
| **Compare to franchise history** | Current Reddit sentiment **[Reddit]** | `reviews.content` **[TMDB]** for previous films<br>(e.g., Dune 1 reviews) | ✅ Fully Supported |
| **Track sentiment velocity** | `score`, `created_utc` (6-hour windows) | N/A | ✅ Fully Supported |
| **Compare to franchise history** | Current Reddit sentiment | `reviews.content` for previous films<br>(e.g., Dune 1 reviews) | ✅ Fully Supported |

**Example Use Case:**
```
Query: "Is Dune 2 having a PR crisis?"

Reddit API [Speed Layer]:
- GET /r/movies/new?q=Dune → 450 posts in last 24h [Reddit]
- GET /comments/{article} → Extract 2,000 comment texts [Reddit]
- VADER analysis on Reddit data → Current sentiment: +0.3

TMDB API [Batch Layer]:
- GET /genre/movie/list → Genre ID for Sci-Fi [TMDB]
- GET /movie/{dune_1_id}/reviews → 5,000 reviews [TMDB] (avg sentiment +0.78)
- GET /discover/movie?with_genres=878 → 100 sci-fi films [TMDB]
- Batch VADER on all TMDB sci-fi reviews → Genre baseline: +0.65

Merge [Serving Layer]:
Current +0.3 [Reddit] vs baseline +0.65 [TMDB] = -0.35 deviation (beyond ±0.15 threshold)
→ PR CRISIS ALERT
```

---

## Business Goal #2: Viral Content Identification for Marketing Amplification

**Goal**: Identify breakout content by comparing real-time Reddit engagement velocity against historical viral thresholds

### Reddit API (Speed Layer) - Available Features

| **Endpoint** | **Data Fields** | **Supports Task** |
|-------------|----------------|-------------------|
| `GET /r/{subreddit}/new` | • `score` (current upvotes) **[Reddit]**<br>• `created_utc` (post age) **[Reddit]**<br>• `num_comments` **[Reddit]**<br>• `gilded` (gold awards count) **[Reddit]**<br>• `all_awardings` (array of awards) **[Reddit]** | ✅ **CRITICAL** - Upvote velocity tracking |
| `GET /r/{subreddit}/hot` | • Same as `/new` but sorted by hot algorithm **[Reddit]** | ✅ Identify trending posts |
| `GET /duplicates/{article}` | • List of cross-posts to other subreddits **[Reddit]** | ✅ Cross-subreddit spread tracking |
| `POST /api/info?url={url}` | • All submissions with same URL **[Reddit]** | ✅ Track same content across subreddits |
| `GET /api/morechildren` | • Expanded comment threads **[Reddit]** | ✅ Comment acceleration tracking |

**Key Features for Viral Detection:**
- ✅ **Upvote Velocity**: `score` **[Reddit]** ÷ (`current_time` - `created_utc` **[Reddit]**) → Upvotes per hour
- ✅ **Award Velocity**: `all_awardings` **[Reddit]** count ÷ post age → Gold/Platinum per hour
- ✅ **Cross-Subreddit Spread**: `duplicates` **[Reddit]** endpoint → Track r/movies → r/all spread
- ✅ **Comment Acceleration**: `num_comments` **[Reddit]** over 5-min windows → New comments per interval
- ✅ **Temporal Ratio**: Post age vs engagement (young + high engagement = viral) **[Reddit]**

**Rate Limit**: 60 requests/minute (free tier)

**Reddit Award Types** (via `all_awardings`):
- `gold` (100 coins)
- `platinum` (700 coins)
- `silver` (free)
- Custom awards (subreddit-specific)

---

### TMDB API (Batch Layer) - Available Features

| **Endpoint** | **Data Fields** | **Supports Task** |
|-------------|----------------|-------------------|
| `GET /movie/{movie_id}` | • `vote_count` (cumulative votes) **[TMDB]**<br>• `popularity` (TMDB popularity score) **[TMDB]**<br>• `budget`, `revenue` **[TMDB]**<br>• `genres` **[TMDB]** | ✅ Budget tier classification (indie vs blockbuster) |
| `GET /movie/{movie_id}/changes` | • `changes.vote_count` (vote count deltas over time) **[TMDB]** | ⚠️ **Partial** - Limited historical velocity data |
| `GET /trending/movie/{time_window}` | • `time_window`: "day" or "week" **[TMDB]**<br>• List of trending movies **[TMDB]** | ✅ Identify historical viral events |
| `GET /movie/popular` | • Popular movies with vote counts **[TMDB]** | ✅ Calculate top quartile thresholds |
| `GET /discover/movie` | • Filter by `vote_count.gte`, `vote_average.gte` **[TMDB]**<br>• `with_genres` (genre filter) **[TMDB]** | ✅ **CRITICAL** - Genre-specific viral thresholds |

**Key Features for Viral Thresholds:**
- ✅ **Vote Velocity (Proxy)**: `vote_count` **[TMDB]** changes via `/changes` endpoint → Historical vote patterns
- ✅ **Genre Baselines**: `/discover/movie?with_genres=878&vote_count.gte=1000` **[TMDB]** → Sci-fi viral threshold
- ✅ **Budget Tier Context**: `budget` **[TMDB]** → Indie ($<20M) vs Mid ($20-100M) vs Blockbuster (>$100M)
- ✅ **Historical Viral Cases**: `/trending/movie/week` **[TMDB]** → Study "Everything Everywhere All At Once" peak patterns
- ⚠️ **Limitation**: TMDB doesn't track hour-by-hour vote velocity (Reddit does)

**Rate Limit**: 4 requests/second

---

### Combined Feature Support for Goal #2

| **Task** | **Reddit Features Used** | **TMDB Features Used** | **Status** |
|---------|-------------------------|------------------------|-----------|
| **Track upvote velocity** | `score` **[Reddit]** ÷ post age (upvotes/hour) | N/A | ✅ Fully Supported |
| **Calculate viral thresholds** | N/A | `/discover/movie` **[TMDB]** + genre filter<br>`vote_count` **[TMDB]** 99th percentile | ✅ Fully Supported |
| **Monitor cross-subreddit spread** | `/duplicates/{article}` **[Reddit]** (count subreddits) | N/A | ✅ Fully Supported |
| **Track award velocity** | `all_awardings` **[Reddit]** count ÷ post age | N/A | ✅ Fully Supported |
| **Budget tier analysis** | N/A | `budget`, `revenue` **[TMDB]** (tier classification) | ✅ Fully Supported |
| **Historical viral case studies** | N/A | `/trending/movie/week` **[TMDB]** (past trends) | ⚠️ Limited historical data |

**Example Use Case:**
```
Query: "Is 'The Creator' going viral?"

Reddit API [Speed Layer]:
- GET /r/movies/new?q=The Creator → Post ID: abc123 [Reddit]
- GET /r/movies/comments/abc123 → score: 5,000 [Reddit], created_utc: 10h ago [Reddit]
- Upvote velocity: 5000 / 10 = 500 upvotes/hour [Reddit]
- GET /duplicates/abc123 → Cross-posted to 5 subreddits [Reddit] (r/scifi, r/movies, r/boxoffice, r/TrueFilm, r/Futurology)
- all_awardings: 30 gold + 3 platinum [Reddit] in 10 hours → 3.3 awards/hour

TMDB API [Batch Layer]:
- GET /movie/{the_creator_id} → budget: $80M [TMDB], genres: [878 (Sci-Fi)] [TMDB]
- GET /discover/movie?with_genres=878&sort_by=vote_count.desc → Top 1000 sci-fi films [TMDB]
- Calculate: 50th percentile = 50 votes/hour, 75th = 150, 99th = 300 [TMDB]
- Budget tier: $80M = Mid-budget (2.5x coefficient for breakout) [TMDB]

Merge [Serving Layer]:
Current 500/hour [Reddit] vs genre viral threshold 300/hour [TMDB] (99th percentile)
→ 500 / 300 = 1.67x above viral threshold
→ VIRAL EVENT CONFIRMED
```

---

## Business Goal #3: Content Recommendation Optimization

**Goal**: Provide competitive context for content performance by combining real-time Reddit engagement with historical TMDB review patterns

### Reddit API (Speed Layer) - Available Features

| **Endpoint** | **Data Fields** | **Supports Task** |
|-------------|----------------|-------------------|
| `GET /r/{subreddit}/new` | • `title`, `selftext` **[Reddit]**<br>• `score`, `num_comments` **[Reddit]**<br>• `all_awardings` **[Reddit]**<br>• `created_utc` **[Reddit]** | ✅ Current discussion volume tracking |
| `GET /r/{subreddit}/search` | • `q` parameter (search query) **[Reddit]**<br>• `sort` (relevance, new, top) **[Reddit]**<br>• `t` (time filter: hour, day, week) **[Reddit]** | ✅ **CRITICAL** - Compare competing releases |
| `GET /r/{subreddit}/hot` | • Trending discussions **[Reddit]** | ✅ Buzz intensity ranking |
| `GET /api/morechildren` | • Comment threads **[Reddit]** | ✅ Discussion quality (spoiler vs non-spoiler analysis) |

**Key Features for Competitive Intelligence:**
- ✅ **Multi-Film Comparison**: `/search?q=Barbie` vs `/search?q=Oppenheimer` **[Reddit]** → Same-weekend comparison
- ✅ **Discussion Volume**: `num_comments` **[Reddit]** across multiple films
- ✅ **Engagement Quality**: `all_awardings` **[Reddit]** ÷ post count → Awards per post (strong reaction indicator)
- ✅ **Community Buzz**: `score` **[Reddit]** distribution across competing films
- ✅ **Topic Extraction**: Parse `selftext` **[Reddit]** + `body` **[Reddit]** for spoilers, specific scenes (NLP required)

**Rate Limit**: 60 requests/minute (free tier)

---

### TMDB API (Batch Layer) - Available Features

| **Endpoint** | **Data Fields** | **Supports Task** |
|-------------|----------------|-------------------|
| `GET /movie/{movie_id}/reviews` | • `content` (review text) **[TMDB]**<br>• `author_details.rating` (1-10) **[TMDB]**<br>• `created_at` **[TMDB]** | ✅ **CRITICAL** - Historical review archive for baselines |
| `GET /discover/movie` | • `with_genres` (genre filter) **[TMDB]**<br>• `primary_release_date.gte/lte` (release timing) **[TMDB]**<br>• `sort_by` (vote_count, popularity, revenue) **[TMDB]** | ✅ **CRITICAL** - Cross-movie comparisons |
| `GET /movie/{movie_id}/similar` | • List of similar films by TMDB algorithm **[TMDB]** | ✅ Find comparable films for context |
| `GET /movie/{movie_id}` | • `belongs_to_collection` (franchise info) **[TMDB]**<br>• `genres`, `runtime`, `budget` **[TMDB]** | ✅ Franchise trajectory analysis |
| `GET /genre/movie/list` | • Genre names and IDs **[TMDB]** | ✅ Genre evolution trends |

**Key Features for Historical Context:**
- ✅ **Review Archive**: `/movie/{id}/reviews` **[TMDB]** → Complete review history (5 years)
- ✅ **Cross-Movie Comparison**: `/discover/movie?primary_release_date.gte=2023-07-01&primary_release_date.lte=2023-07-31` **[TMDB]** → Same-month releases
- ✅ **Release Timing Analysis**: `primary_release_date` **[TMDB]** → Summer blockbusters vs award season
- ✅ **Genre Evolution**: Historical genre sentiment trends **[TMDB]** (comedy 2020: +0.75 → 2023: +0.68)
- ✅ **Franchise Patterns**: `/movie/{toy_story_4_id}/reviews` **[TMDB]** → Sequel performance vs originals

**Rate Limit**: 4 requests/second

---

### Combined Feature Support for Goal #3

| **Task** | **Reddit Features Used** | **TMDB Features Used** | **Status** |
|---------|-------------------------|------------------------|-----------|
| **Compare competing releases** | `/search?q=Barbie` vs `/search?q=Oppenheimer` **[Reddit]**<br>`score`, `num_comments` **[Reddit]** (current buzz) | `/discover/movie` **[TMDB]** (same release date)<br>`reviews.content` **[TMDB]** (historical performance) | ✅ Fully Supported |
| **Track discussion volume** | `num_comments` **[Reddit]** across multiple films | N/A | ✅ Fully Supported |
| **Engagement quality analysis** | `all_awardings` **[Reddit]** ÷ post count | N/A | ✅ Fully Supported |
| **Genre baselines** | N/A | `/discover/movie?with_genres=35` **[TMDB]** (comedy)<br>Historical sentiment aggregation **[TMDB]** | ✅ Fully Supported |
| **Franchise trajectory** | N/A | `/movie/{id}/reviews` **[TMDB]** for sequels vs originals | ✅ Fully Supported |
| **Release timing context** | N/A | `primary_release_date` **[TMDB]** (seasonal patterns) | ✅ Fully Supported |
| **Topic extraction** | Parse `selftext`, `body` **[Reddit]** for keywords | N/A | ⚠️ Requires NLP (not API feature) |

**Example Use Case:**
```
Query: "Should we prioritize Barbie in recommendations?"

Reddit API [Speed Layer]:
- GET /r/movies/search?q=Barbie&t=week → 150 posts [Reddit]
- Aggregate: 2,000 comments/day [Reddit], avg sentiment +0.9, 1,800 awards [Reddit] (12 awards/post)
- GET /r/movies/search?q=Oppenheimer&t=week → 100 posts [Reddit]
- Aggregate: 800 comments/day [Reddit], avg sentiment +0.92, 600 awards [Reddit] (6 awards/post)

TMDB API [Batch Layer]:
- GET /genre/movie/list → Comedy ID: 35 [TMDB]
- GET /discover/movie?with_genres=35&primary_release_date.gte=2023-01-01 → 200 comedies [TMDB]
- Batch VADER on all comedy reviews [TMDB] → Genre baseline: +0.71 (down from 2020: +0.75)
- GET /movie/{toy_story_4_id}/reviews → Historical comp: +0.85, 3,400 reviews [TMDB]
- GET /discover/movie?primary_release_date.gte=2023-07-01&primary_release_date.lte=2023-07-31 [TMDB] → July releases face 15% higher competition

Merge [Serving Layer]:
Barbie: 2,000 comments/day [Reddit] (+0.9 sentiment) vs Oppenheimer: 800 comments/day [Reddit] (+0.92 sentiment)
Barbie sentiment +0.9 beats genre baseline +0.71 [TMDB] by +0.19
Exceeds declining trend (+0.68 in 2023 [TMDB])
Higher engagement quality (12 awards/post [Reddit] vs 6)
→ PRIORITIZE BARBIE - Rare Dual Success (beats both buzz [Reddit] AND historical quality benchmarks [TMDB])
```

---

## Summary: Feature Coverage by Business Goal

| **Business Goal** | **Reddit API Support** | **TMDB API Support** | **Overall Status** |
|-------------------|----------------------|---------------------|-------------------|
| **#1: PR Crisis Detection** | ✅ Real-time sentiment<br>✅ Velocity tracking<br>✅ Discussion volume | ✅ Historical review archive<br>✅ Genre baselines<br>✅ Statistical thresholds | ✅ **Fully Supported** |
| **#2: Viral Content ID** | ✅ Upvote velocity<br>✅ Cross-subreddit spread<br>✅ Award velocity | ✅ Genre-specific thresholds<br>✅ Budget tier analysis<br>⚠️ Limited historical velocity | ✅ **Fully Supported** |
| **#3: Recommendation Optimization** | ✅ Multi-film comparison<br>✅ Engagement quality<br>✅ Discussion volume | ✅ Review archive<br>✅ Cross-movie comparison<br>✅ Franchise patterns | ✅ **Fully Supported** |

---

## API Limitations & Workarounds

### Reddit API Limitations
| **Limitation** | **Impact** | **Workaround** |
|---------------|-----------|---------------|
| 60 requests/minute (free tier) | Slow data ingestion | Polling strategy: 30-second intervals, prioritize r/movies, r/boxoffice, r/TrueFilm |
| No historical data access | Can't compute Reddit baselines | Use TMDB for historical context; Reddit for ≤48h data |
| No direct sentiment scores | Must compute ourselves | VADER sentiment analysis on `body`, `selftext` |
| Rate limit resets every minute | Bursts limited to 60 requests | Distribute requests evenly across 60 seconds |

### TMDB API Limitations
| **Limitation** | **Impact** | **Workaround** |
|---------------|-----------|---------------|
| 4 requests/second | Slower batch processing | Batch requests in 4-hour cycles; use `append_to_response` to reduce calls |
| No hour-by-hour vote velocity | Can't track viral momentum | Use Reddit upvote velocity as primary signal; TMDB for thresholds |
| Limited historical trending data | Hard to study past viral events | Use `/discover/movie` + `vote_count` as proxy for past popularity |
| No cross-platform engagement | TMDB data only | Reddit provides social engagement; merge at query time |

---

## Conclusion

**All three business goals are fully supported** by the available API features from Reddit and TMDB. The dual-source architecture leverages:

1. **Reddit API strengths**: Real-time social signals (upvotes, comments, awards, cross-posts)
2. **TMDB API strengths**: Historical review archives, genre baselines, franchise context

**Critical Success Factors**:
- ✅ Reddit provides `score`, `num_comments`, `all_awardings` **[Reddit]** for velocity tracking
- ✅ Reddit provides `body`, `selftext` **[Reddit]** for sentiment analysis input
- ✅ TMDB provides `reviews.content` **[TMDB]** for historical sentiment baselines
- ✅ TMDB provides `genres`, `budget`, `primary_release_date` **[TMDB]** for statistical context
- ✅ 48-hour cutoff strategy works: Reddit **[Speed Layer]** for ≤48h, TMDB **[Batch Layer]** for >48h historical data

**No showstoppers identified**. Both APIs provide sufficient features for all business requirements.

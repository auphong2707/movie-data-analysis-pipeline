# 🎯 Dashboard Plan: Content Recommendation Optimization

## Goal
**Surface trending content by combining fresh Reddit buzz with historical TMDB performance to maximize engagement.**

## Business Value
- Increase user engagement by promoting hot content in real-time
- Balance trending buzz with proven quality metrics
- Provide actionable insights for content curation teams

---

## 📊 Dashboard Layout (4 Rows)

### Row 1: Key Metrics (4 Stats Panels)
**Purpose**: Quick snapshot of recommendation system health

| Panel | Metric | API Endpoint | Visual |
|-------|--------|--------------|--------|
| 🏆 Top Dual-Success Score | Max `dual_success_score` | `/recommendations/dual-success?limit=1` | Stat (threshold: >80=green, 60-80=yellow, <60=red) |
| 🔥 Reddit Buzz Powered | Count where `speed_layer_contribution=true` | `/recommendations/dual-success?limit=100` | Stat (threshold: >20=green, 10-20=yellow, <10=red) |
| 📚 TMDB Quality Leaders | Count where `tmdb_score > 70` | `/recommendations/dual-success?limit=100` | Stat (show percentage) |
| 🎭 Active Genres | Count distinct `genre` values | `/recommendations/dual-success?limit=100` | Stat (info color) |

---

### Row 2: Score Distribution (2 Panels)
**Purpose**: Visualize the balance between Reddit buzz and TMDB quality

#### Panel 1: Dual-Success Scatter Plot
- **Type**: Scatter plot
- **X-axis**: `tmdb_score` (0-100)
- **Y-axis**: `reddit_buzz_score` (0-100)
- **Point size**: `popularity`
- **Point color**: By `genre`
- **Tooltip**: movie_title, dual_success_score, vote_average, reddit_mentions
- **API**: `/recommendations/dual-success?limit=50`
- **Insight**: Shows correlation between buzz and quality

#### Panel 2: Top 10 Dual-Success Rankings
- **Type**: Bar gauge (horizontal)
- **Metric**: `dual_success_score`
- **Label**: `movie_title`
- **Color gradient**: 0-50 (red) → 50-75 (yellow) → 75-100 (green)
- **API**: `/recommendations/dual-success?limit=10`
- **Insight**: Quick view of best recommendations

---

### Row 3: Component Analysis (3 Panels)
**Purpose**: Deep dive into individual scoring components

#### Panel 1: Pure Reddit Buzz Rankings
- **Type**: Table
- **Columns**: rank, movie_title, reddit_buzz_score, total_engagement, reddit_mentions
- **Sort**: By `reddit_buzz_score` DESC
- **API**: `/recommendations/reddit-buzz?days_back=7&limit=15`
- **Color**: reddit_buzz_score with threshold coloring
- **Insight**: What's hot on Reddit right now

#### Panel 2: Pure TMDB Quality Rankings
- **Type**: Table
- **Columns**: rank, movie_title, tmdb_quality_score, vote_average, vote_count
- **Sort**: By `tmdb_quality_score` DESC
- **API**: `/recommendations/tmdb-quality?min_vote_count=100&limit=15`
- **Color**: vote_average with threshold coloring (>8.0=green, 7-8=yellow, <7=orange)
- **Insight**: Proven quality from TMDB

#### Panel 3: Score Component Breakdown (Top 5)
- **Type**: Stacked bar chart
- **Series**:
  - Reddit component: `reddit_buzz_score * 0.6`
  - TMDB component: `tmdb_score * 0.4`
- **X-axis**: `movie_title` (top 5)
- **Y-axis**: Score contribution (0-100)
- **API**: `/recommendations/dual-success?limit=5`
- **Insight**: See 60/40 split visually

---

### Row 4: Genre Performance (2 Panels)
**Purpose**: Genre-specific recommendations and performance

#### Panel 1: Genre Dropdown Filter + Top Movies by Genre
- **Type**: Table with genre variable selector
- **Variable**: `genre` (extracted from API response genres)
- **Columns**: rank, movie_title, dual_success_score, reddit_buzz_score, tmdb_score, vote_average
- **API**: `/recommendations/dual-success/genre/${genre}?limit=10`
- **Dynamic**: Updates based on dropdown selection
- **Default genre**: "Action" or most popular
- **Insight**: Curated recommendations per genre

#### Panel 2: Genre Score Heatmap
- **Type**: Heatmap (if supported) or Table with color cells
- **Rows**: Top 5 genres
- **Columns**: avg_dual_success_score, avg_reddit_buzz, avg_tmdb_score
- **API**: `/recommendations/dual-success?limit=100` (aggregate by genre in panel transform)
- **Color scale**: 0 (cold blue) → 100 (hot red)
- **Insight**: Which genres perform best overall

---

## 🔧 Technical Implementation

### Datasource
- **Primary**: Infinity datasource (HTTP JSON API)
- **Base URL**: `http://serving-api:8000/api/v1`
- **Method**: GET
- **Format**: JSON
- **Refresh**: 5 minutes (configurable)

### API Endpoints Used
1. `/recommendations/dual-success` - Main recommendations (60% Reddit + 40% TMDB)
2. `/recommendations/dual-success/genre/{genre}` - Genre-filtered recommendations
3. `/recommendations/reddit-buzz` - Pure Reddit buzz rankings
4. `/recommendations/tmdb-quality` - Pure TMDB quality rankings
5. `/recommendations/similar?ids={ids}&strategy={strategy}` - User preference-based similar movies
6. `/recommendations/similar/{movie_id}` - Single movie similarity
7. `/utilities/search?q={query}&limit=100` - **Search movies by title** (for query variable)

**Note**: The `/utilities/search` endpoint exists but needs implementation to support the query variable.
Expected response format:
```json
[
  {"movie_id": 299534, "title": "Avengers: Endgame", "year": 2019},
  {"movie_id": 299536, "title": "Avengers: Infinity War", "year": 2018}
]
```
Implementation: MongoDB `$text` search or regex on `title` field, case-insensitive.

### Transformations Needed
- Count aggregations for stat panels
- Max/min calculations for thresholds
- Group by genre for heatmap
- Filter for `speed_layer_contribution=true` count

### Variables
- `$genre`: Dropdown selector for genre filtering (custom variable)
- `$limit`: Text box for result count (default 20, user can modify)
- `$min_rating`: Text box for TMDB rating filter (default 6.0, user can modify)
- `$user_movies`: **Query variable** - Search movies by title, returns IDs automatically - Optional for Row 5
  - Type: Query variable with multi-select
  - Data source: Infinity datasource
  - Query: `/utilities/search?query=` (searches MongoDB by movie title)
  - User types movie title → dropdown shows matches → selects movies → variable contains IDs
- `$similarity_strategy`: **Dropdown** for strategy ("average", "union", "intersection") - Optional for Row 5

**Note on Variables**:
- Grafana **query variables** can fetch data from APIs (including search endpoints)
- User types in search box → API returns matching movies with IDs and titles
- User selects from dropdown → variable stores the movie IDs
- Much more user-friendly than manually entering movie IDs
- When variable changes, all dependent panels automatically refresh with new API calls
- Example: User searches "Avengers" → sees list of Avengers movies → selects 2-3 → gets recommendations

---

## 📈 Dashboard Features

### Auto-Refresh
- **Interval**: 5 minutes
- **Rationale**: Balance between real-time updates and API load

### Time Range
- **Default**: Last 7 days (for Reddit data)
- **Note**: Not time-series data, but recency affects scores

### Thresholds
- **Dual-Success Score**: <60 (🔴), 60-80 (🟡), >80 (🟢)
- **Reddit Buzz**: Low activity (🔴), Moderate (🟡), High (🟢)
- **TMDB Quality**: <7.0 (🟠), 7-8 (🟡), >8 (🟢)

### Color Scheme
- Use consistent Grafana palette
- Genre colors: Unique per genre for scatter plot
- Score gradients: Red → Yellow → Green

---

## 🎨 Visual Design Principles

1. **Simplicity**: Clear labels, minimal clutter
2. **Intuitive**: Scores use familiar 0-100 scale
3. **Actionable**: Tables sorted by relevance, easy to scan
4. **Balanced**: Show both components (Reddit + TMDB) equally
5. **Interactive**: Filters and variables for exploration

---

## 🚀 Implementation Steps

1. Create dashboard JSON with metadata
2. Add Row 1: 4 stat panels with API queries
3. Add Row 2: Scatter plot + bar gauge
4. Add Row 3: 3 comparison tables
5. Add Row 4: Genre selector + heatmap
6. Configure variables and refresh settings
7. Test with real API responses
8. Deploy via dashboard-provider.yml

---

## 🎯 BONUS: User Preference Panel (Optional Row 5)

### Purpose: Personalized recommendations based on user's favorite movies

This panel utilizes the `/recommendations/similar` API to provide **content-based filtering** recommendations. Unlike the dual-success score which surfaces trending+quality content globally, this gives personalized recommendations based on individual user preferences.

#### Panel: Similar Movies Based on User Preferences
- **Type**: Table
- **Grafana Variables** (at dashboard top):
  - `$user_movies` - **Query variable** with multi-select (much better than manual IDs!)
    - Data source: Infinity datasource
    - Query URL: `http://serving-api:8000/api/v1/utilities/search?q=&limit=100`
    - Returns: Array of `{movie_id, title}` objects
    - Value field: `movie_id`
    - Display field: `title`
    - Multi-select: Enabled (users can pick 2-5 movies)
    - Label: "Select Your Favorite Movies"
    - User experience: Type to search → select movies from dropdown → IDs stored in variable
  - `$similarity_strategy` - Custom variable (dropdown)
    - Options: `average`, `union`, `intersection`
    - Default: `average`
    - Label: "Recommendation Strategy"

**Columns**:
- rank, movie_title, similarity_score, shared_genre, release_year_diff
- popularity, vote_average, matched_with

**API**: `/recommendations/similar?ids=${user_movies:csv}&strategy=${similarity_strategy}&limit=15`
- Note: `${user_movies:csv}` formats multi-select as comma-separated values

**How Users Interact**:
1. User types movie title in `$user_movies` search box (e.g., "Avengers")
2. Dropdown shows matching movies from database (Endgame, Infinity War, etc.)
3. User selects 2-5 favorite movies from the list
4. User selects `$similarity_strategy` from dropdown
5. Dashboard auto-refreshes with new API call using selected movie IDs
6. Panel shows personalized recommendations

**Search Endpoint Requirements**:
- `/utilities/search?q={query}&limit=100` must return:
  ```json
  [
    {"movie_id": 299534, "title": "Avengers: Endgame"},
    {"movie_id": 299536, "title": "Avengers: Infinity War"}
  ]
  ```
- Searches movie titles in MongoDB (case-insensitive partial match)
- Returns top 100 matches for dropdown population

**Similarity Algorithm** (Content-Based Filtering):
- **Genre matching** (exact match)
- **Director matching** (same director)
- **Franchise matching** (same franchise/series)
- **Budget tier similarity** (indie/mid/blockbuster adjacency)
- **Release year proximity** (exponential decay: 5-year half-life)

**Color coding**:
- similarity_score: >0.8 (🟢), 0.6-0.8 (🟡), <0.6 (🟠)
- Highlight rows where `matched_with ≥ 2` (for multi-movie input)

**Note**: This is a **demonstration panel** showing API capability. Real production use would require:
- External application to collect user movie IDs (e.g., web app with user profiles)
- Deep link to Grafana with pre-filled variables: `?var-user_movie_ids=299534,157336`
- Or: Use as internal tool for data team to test similarity recommendations

---

### Use Cases for Similar Movies API

#### 1. **"More Like This" Feature**
- User clicks on a movie → Call `/recommendations/similar/{movie_id}`
- Shows similar movies based on content attributes (genre, director, franchise)
- Perfect for "If you liked X, you'll love Y"

#### 2. **User Profile-Based Recommendations**
- User has liked/watched multiple movies → Extract their movie IDs
- Call `/recommendations/similar?ids=1,2,3&strategy=average`
- Get personalized recommendations blending all their preferences

#### 3. **Taste Clustering**
- **Union strategy**: Explore diverse recommendations
  - User likes both Marvel and Horror → Get both superhero AND scary movies
- **Intersection strategy**: Narrow down to specific niche
  - User likes Nolan films AND sci-fi → Get cerebral sci-fi recommendations
- **Average strategy**: Balanced middle ground

#### 4. **Integration with Dual-Success**
**Two-stage recommendation pipeline**:
1. **Stage 1 (Global)**: Use `/recommendations/dual-success` to find trending+quality movies
2. **Stage 2 (Personalized)**: Filter by user preference using `/recommendations/similar`

Example workflow:
```
User has liked: [Inception, Interstellar] (sci-fi, Nolan films)

Step 1: Get global trending content
→ /recommendations/dual-success?genre=Science Fiction&limit=50

Step 2: For each trending movie, calculate similarity to user's preferences
→ /recommendations/similar?ids=27205,157336&limit=50

Step 3: Merge results: dual_success_score × similarity_score
→ Surface movies that are BOTH trending AND match user taste
```

#### 5. **Cold Start Problem Solution**
- New user with no history? Use dual-success (trending+quality)
- User with 1-2 liked movies? Use similar movies with union strategy
- User with 5+ liked movies? Use similar movies with average strategy
- Power user with many ratings? Combine dual-success filtering with similarity

---

### Dashboard Integration Example

**Grafana Variables (Dashboard-level)**

At the top of the dashboard, users see:

```
Variables:
┌──────────────────────────────────────────────────────────────┐
│ Genre: [Action ▼]  Min Rating: [6.0]  Limit: [20]           │
│                                                              │
│ Your Movies: [Type to search...          ▼]  (Multi-select)│
│              [Avengers: Endgame ✓] [Interstellar ✓]        │
│ Strategy: [average ▼]                                       │
└──────────────────────────────────────────────────────────────┘
```

**How It Works**:
- **Query variables** fetch data from API endpoints (autocomplete search!)
- User types "Avengers" → API searches database → dropdown shows matches
- User selects movies → variable stores selected movie IDs (not titles)
- When user changes a variable, affected panels automatically refresh
- Much better UX than manually typing movie IDs!

**Example Variable Definitions** (dashboard JSON):

```json
"templating": {
  "list": [
    {
      "name": "user_movies",
      "type": "query",
      "label": "Your Favorite Movies",
      "multi": true,
      "datasource": {
        "type": "yesoreyeram-infinity-datasource",
        "uid": "infinity"
      },
      "query": {
        "type": "json",
        "source": "url",
        "url": "http://serving-api:8000/api/v1/utilities/search?q=&limit=100",
        "format": "table"
      },
      "regex": "/(.*)/",
      "refresh": 1,
      "current": {
        "selected": true,
        "text": ["Avengers: Endgame", "Interstellar"],
        "value": ["299534", "157336"]
      }
    },
    {
      "name": "similarity_strategy",
      "type": "custom",
      "query": "average,union,intersection",
      "current": {"value": "average"}
    }
  ]
}
```

**Panel Using Query Variable**:

```json
{
  "targets": [
    {
      "url": "http://serving-api:8000/api/v1/recommendations/similar?ids=${user_movies:csv}&strategy=${similarity_strategy}&limit=15",
      "format": "table",
      "root_selector": "similar_movies"
    }
  ]
}
```

**Key Points**:
- `${user_movies:csv}` formats the array as comma-separated: `299534,157336`
- Query variables refresh on dashboard load or manual refresh
- Multi-select allows choosing 2-5 movies (better recommendations)
- Search is powered by `/utilities/search` endpoint

**Limitations & Solutions**:
- ❌ No live search-as-you-type → ✅ Query variable with refresh button
- ❌ Requires `/utilities/search` endpoint → ✅ Already exists (needs implementation)
- ✅ Much better than text box with manual IDs!
- ✅ Variables can be shared via URL: `?var-user_movies=299534&var-user_movies=157336`

---

### Comparison: Dual-Success vs Similar Movies

| Aspect | Dual-Success | Similar Movies |
|--------|--------------|----------------|
| **Goal** | Surface globally trending+quality content | Personalize to individual user taste |
| **Input** | None (global rankings) | User's liked movie IDs |
| **Algorithm** | 60% Reddit buzz + 40% TMDB quality | Cosine similarity on content features |
| **Use Case** | Homepage "Trending Now" section | "Recommended For You" section |
| **Speed Layer** | ✅ Uses Reddit real-time data | ❌ Batch layer only (content features) |
| **Personalization** | ❌ Same for all users | ✅ Unique per user |
| **Cold Start** | ✅ Works for new users | ❌ Needs user history |
| **Freshness** | ✅ Surfaces newest trending | 🟡 Based on content similarity |

**Best Practice**: Use BOTH in your recommendation system:
- **Homepage**: Dual-success for trending content everyone should see
- **Profile Page**: Similar movies for personalized "More Like This"
- **Hybrid**: Dual-success filtered by similarity to user preferences

---

## 📊 Expected Insights

**For Content Teams:**
- Which movies to promote NOW (high dual-success score)
- Reddit trending vs. proven quality comparison
- Genre-specific recommendations for targeted marketing

**For Data Teams:**
- Validate 60/40 weighting is optimal
- Identify movies with mismatched Reddit/TMDB scores
- Monitor speed layer contribution percentage

**For Business:**
- Real-time content curation decisions
- Balance viral buzz with quality metrics
- Maximize engagement by surfacing hot content early

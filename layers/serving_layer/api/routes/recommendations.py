"""
Recommendation Routes - Goal #3: Content Recommendation Optimization
"""
from fastapi import APIRouter, HTTPException, Query, Depends
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
import logging
import math

from api.schemas.recommendations import (
    DualSuccessResponse,
    DualSuccessRecommendation,
    InputMovie,
    SimilarMoviesResponse,
    SimilarMovieRecommendation,
    RedditBuzzRecommendation,
    RedditBuzzResponse,
    TMDBQualityResponse
)
from mongodb.client import get_mongodb_client, get_database
from mongodb.queries import MovieQueries
from query_engine.similarity_engine import (
    build_feature_vector,
    calculate_similarity_score
)

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/recommendations",
    tags=["recommendations"]
)


def get_movie_queries() -> MovieQueries:
    """Dependency to get MovieQueries instance"""
    db = get_database()
    return MovieQueries(db)


def calculate_recency_weight(age_hours: float) -> float:
    """
    Calculate recency weight based on age in hours
    
    Args:
        age_hours: Age of the discussion in hours
    
    Returns:
        Recency weight (0.2 to 1.0)
    """
    if age_hours <= 24:
        return 1.0
    elif age_hours <= 48:
        return 0.8
    elif age_hours <= 168:  # 7 days
        return 0.6
    elif age_hours <= 720:  # 30 days
        return 0.4
    else:
        return 0.2


def normalize_scores(values: List[float]) -> List[float]:
    """
    Normalize a list of values to 0-100 scale
    
    Args:
        values: List of raw score values
    
    Returns:
        List of normalized scores (0-100)
    """
    if not values or len(values) == 0:
        return []
    
    min_val = min(values)
    max_val = max(values)
    
    # Handle edge case where all values are the same
    if max_val == min_val:
        return [50.0] * len(values)
    
    return [(v - min_val) / (max_val - min_val) * 100 for v in values]


@router.get("/dual-success", response_model=DualSuccessResponse)
async def get_dual_success_recommendations(
    genre: Optional[str] = Query(None, description="Filter by genre"),
    min_rating: float = Query(6.0, ge=0, le=10, description="Minimum TMDB rating"),
    limit: int = Query(20, ge=1, le=100, description="Maximum number of results"),
    queries: MovieQueries = Depends(get_movie_queries)
):
    """
    Get dual-success recommendations (60% Reddit buzz + 40% TMDB quality)
    
    Formula calibrated on 3,657 movies from production database (Dec 18, 2025).
    See API_DASHBOARD_REORGANIZATION_PLAN.md for complete formula details.
    """
    try:
        logger.info(f"Fetching dual-success recommendations: genre={genre}, min_rating={min_rating}, limit={limit}")
        
        # Step 1: Get batch layer movies (movie_intelligence collection)
        batch_movies = queries.get_batch_movies_for_recommendations(
            min_rating=min_rating,
            genre=genre,
            min_popularity=1.0,
            min_vote_count=50
        )
        
        if not batch_movies:
            return DualSuccessResponse(
                recommendations=[],
                total_count=0,
                filters_applied={
                    "genre": genre,
                    "min_rating": min_rating,
                    "limit": limit
                }
            )
        
        logger.info(f"Retrieved {len(batch_movies)} movies from batch layer")
        
        # Step 2: Get speed layer engagement data
        movie_titles = [movie['title'] for movie in batch_movies]
        speed_engagement = queries.get_speed_layer_engagement(
            movie_titles=movie_titles,
            days_back=30
        )
        
        logger.info(f"Retrieved speed layer data for {len(speed_engagement)} movies")
        
        # Step 3: Calculate raw scores for all movies
        movie_scores = []
        now = datetime.utcnow()
        
        for movie in batch_movies:
            movie_title = movie['title']
            
            # Calculate Reddit Score using same formula as reddit-buzz endpoint
            reddit_raw = 0.0
            has_speed_data = False
            discussion_count = 0
            
            if movie_title in speed_engagement:
                has_speed_data = True
                engagement = speed_engagement[movie_title]
                
                # Calculate weighted engagement (same as reddit-buzz endpoint)
                upvotes = engagement.get('total_upvotes', 0)
                comments = engagement.get('total_comments', 0)
                awards = engagement.get('total_awards', 0)
                discussion_count = engagement.get('discussion_count', 0)
                
                W = upvotes + (comments * 2) + (awards * 10)
                
                # Only calculate score if there's meaningful engagement
                if W > 0:
                    # Calculate recency decay (exponential with 24h half-life)
                    last_window = engagement.get('last_window_start')
                    if last_window:
                        age_hours = (now - last_window).total_seconds() / 3600
                    else:
                        age_hours = 30 * 24  # Assume oldest (30 days)
                    
                    decay = math.exp(-age_hours / 24)
                    
                    # Calculate volume multiplier (log scale)
                    post_count = engagement.get('post_count', 1)
                    multiplier = 1 + math.log10(post_count + 1) / 10
                    
                    # Final Reddit raw score (same as reddit-buzz endpoint)
                    reddit_raw = W * decay * multiplier
            
            # Calculate TMDB Score (raw) - Calibrated hybrid formula
            # Components: 50% popularity + 30% quality + 20% credibility
            popularity = movie.get('popularity', 0)
            vote_average = movie.get('vote_average', 0)
            vote_count = movie.get('vote_count', 0)
            
            tmdb_raw = (
                0.5 * popularity +
                0.3 * (vote_average * 10) +
                0.2 * math.log10(vote_count + 1)
            )
            
            # Store movie with raw scores
            movie_scores.append({
                'movie': movie,
                'reddit_raw': reddit_raw,
                'tmdb_raw': tmdb_raw,
                'has_speed_data': has_speed_data,
                'discussion_count': discussion_count
            })
        
        # Step 4: Normalize scores to 0-100 scale
        reddit_raws = [m['reddit_raw'] for m in movie_scores]
        tmdb_raws = [m['tmdb_raw'] for m in movie_scores]
        
        reddit_normalized = normalize_scores(reddit_raws)
        tmdb_normalized = normalize_scores(tmdb_raws)
        
        # Step 5: Calculate dual-success scores and build recommendations
        recommendations = []
        
        for i, movie_score in enumerate(movie_scores):
            movie = movie_score['movie']
            reddit_score = reddit_normalized[i]
            tmdb_score = tmdb_normalized[i]
            
            # Calculate dual-success score: 60% Reddit + 40% TMDB
            dual_success_score = (0.6 * reddit_score) + (0.4 * tmdb_score)
            
            # Get primary genre (handle both flat and array)
            genre_value = movie.get('genre')
            if not genre_value and 'genres' in movie:
                genres = movie.get('genres', [])
                genre_value = genres[0] if genres else None
            
            recommendations.append({
                'movie_id': movie.get('movie_id'),
                'movie_title': movie.get('title'),
                'genre': genre_value,
                'dual_success_score': round(dual_success_score, 1),
                'reddit_buzz_score': round(reddit_score, 1),
                'tmdb_score': round(tmdb_score, 1),
                'vote_average': movie.get('vote_average'),
                'vote_count': movie.get('vote_count'),
                'popularity': movie.get('popularity'),
                'reddit_mentions': movie_score['discussion_count'],
                'speed_layer_contribution': movie_score['has_speed_data']
            })
        
        # Step 6: Sort by dual-success score and assign ranks
        sorted_recs = sorted(recommendations, key=lambda x: x['dual_success_score'], reverse=True)
        
        # Apply limit and assign ranks
        final_recs = []
        for i, rec in enumerate(sorted_recs[:limit]):
            rec['rank'] = i + 1
            final_recs.append(DualSuccessRecommendation(**rec))
        
        logger.info(f"Returning {len(final_recs)} dual-success recommendations")
        
        return DualSuccessResponse(
            recommendations=final_recs,
            total_count=len(final_recs),
            filters_applied={
                "genre": genre,
                "min_rating": min_rating,
                "limit": limit
            }
        )
    
    except Exception as e:
        logger.error(f"Error in dual-success recommendations: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get("/dual-success/genre/{genre}", response_model=DualSuccessResponse)
async def get_dual_success_by_genre(
    genre: str,
    min_rating: float = Query(6.0, ge=0, le=10, description="Minimum TMDB rating"),
    limit: int = Query(20, ge=1, le=100, description="Maximum number of results"),
    queries: MovieQueries = Depends(get_movie_queries)
):
    """
    Get dual-success recommendations for specific genre
    
    Special case: Use genre="All" to get recommendations from all genres (no genre filter)
    """
    # Handle "All" genre by passing None to get all genres
    genre_filter = None if genre.lower() == "all" else genre
    
    return await get_dual_success_recommendations(
        genre=genre_filter,
        min_rating=min_rating,
        limit=limit,
        queries=queries
    )


@router.get("/similar/{movie_id}", response_model=SimilarMoviesResponse)
async def get_similar_movies_by_id(
    movie_id: int,
    limit: int = Query(10, ge=1, le=50, description="Number of similar movies to return"),
    strategy: str = Query("average", description="Similarity strategy (only applies if using ids param)"),
    queries: MovieQueries = Depends(get_movie_queries)
):
    """
    Get content-based similar movies using cosine similarity
    
    Single movie similarity based on:
    - Genre matching
    - Director matching
    - Franchise matching
    - Budget tier similarity
    - Release year proximity
    """
    return await get_similar_movies_impl([movie_id], limit, "average", queries)


@router.get("/similar", response_model=SimilarMoviesResponse)
async def get_similar_movies_by_ids(
    ids: str = Query(..., description="Comma-separated list of movie IDs"),
    limit: int = Query(10, ge=1, le=50, description="Number of similar movies to return"),
    strategy: str = Query("average", regex="^(average|union|intersection)$", description="How to combine multiple movies"),
    queries: MovieQueries = Depends(get_movie_queries)
):
    """
    Get content-based similar movies for multiple input movies
    
    Strategies:
    - average: Balanced recommendations across all liked movies
    - union: Find movies similar to ANY input movie (diverse)
    - intersection: Find movies similar to ALL input movies (focused)
    """
    try:
        # Parse comma-separated IDs
        movie_ids = [int(id.strip()) for id in ids.split(',')]
        if not movie_ids:
            raise HTTPException(status_code=422, detail="At least one movie ID required")
        
        return await get_similar_movies_impl(movie_ids, limit, strategy, queries)
    
    except ValueError:
        raise HTTPException(status_code=422, detail="Invalid movie ID format. Use comma-separated integers.")


async def get_similar_movies_impl(
    movie_ids: List[int],
    limit: int,
    strategy: str,
    queries: MovieQueries
):
    """
    Implementation for similar movies recommendation
    
    Args:
        movie_ids: List of input movie IDs
        limit: Number of recommendations
        strategy: "average", "union", or "intersection"
        queries: MovieQueries instance
    """
    try:
        logger.info(f"Fetching similar movies for {movie_ids}, strategy={strategy}, limit={limit}")
        
        # Get target movies
        targets = queries.get_movies_by_ids(movie_ids)
        
        if not targets:
            raise HTTPException(status_code=404, detail="No valid movies found with provided IDs")
        
        # Build feature vectors for target movies
        target_vecs = [build_feature_vector(movie) for movie in targets]
        target_sentiments = [movie.get('avg_sentiment') for movie in targets]
        
        # Prepare candidate search criteria
        exclude_ids = movie_ids
        
        if len(targets) == 1:
            # Single movie: narrow search
            target = targets[0]
            genres = [target.get('genre')] if target.get('genre') else None
            year = target.get('release_year')
            year_min = year - 3 if year else None
            year_max = year + 3 if year else None
        else:
            # Multiple movies: broader search
            all_genres = list(set([t.get('genre') for t in targets if t.get('genre')]))
            genres = all_genres if all_genres else None
            all_years = [t.get('release_year') for t in targets if t.get('release_year')]
            if all_years:
                year_min = min(all_years) - 3
                year_max = max(all_years) + 3
            else:
                year_min = None
                year_max = None
        
        # Get candidate movies
        candidates = queries.get_candidate_movies_for_similarity(
            exclude_ids=exclude_ids,
            genres=genres,
            year_min=year_min,
            year_max=year_max,
            limit=500
        )
        
        if not candidates:
            return SimilarMoviesResponse(
                input_movies=[InputMovie(movie_id=m['movie_id'], movie_title=m['title']) for m in targets],
                strategy=strategy,
                similar_movies=[],
                total_count=0
            )
        
        logger.info(f"Evaluating {len(candidates)} candidate movies")
        
        # Calculate similarities
        similarities = []
        for candidate in candidates:
            candidate_vec = build_feature_vector(candidate)
            candidate_sentiment = candidate.get('avg_sentiment')
            
            # Calculate similarity score
            sim = calculate_similarity_score(
                target_vecs,
                candidate_vec,
                strategy,
                target_sentiments,
                candidate_sentiment
            )
            
            # Determine shared attributes
            shared_genres = [t.get('genre') for t in targets if t.get('genre') == candidate.get('genre')]
            shared_genre = shared_genres[0] if shared_genres else None
            
            # Calculate year difference from closest target
            year_diffs = []
            for t in targets:
                if t.get('release_year') and candidate.get('release_year'):
                    year_diffs.append(abs(t['release_year'] - candidate['release_year']))
            release_year_diff = min(year_diffs) if year_diffs else None
            
            # For multi-movie: count how many target movies it matches well with
            matched_with = None
            if len(targets) > 1:
                matches = 0
                for tv in target_vecs:
                    test_sim = calculate_similarity_score([tv], candidate_vec, "average", None, None)
                    if test_sim > 0.5:
                        matches += 1
                matched_with = matches
            
            similarities.append({
                'movie_id': candidate['movie_id'],
                'movie_title': candidate['title'],
                'similarity_score': round(sim, 3),
                'shared_genre': shared_genre,
                'release_year_diff': release_year_diff,
                'popularity': candidate.get('popularity', 0),
                'vote_average': candidate.get('vote_average', 0),
                'vote_count': candidate.get('vote_count', 0),
                'matched_with': matched_with
            })
        
        # Sort by similarity score
        sorted_sims = sorted(similarities, key=lambda x: x['similarity_score'], reverse=True)[:limit]
        
        # Assign ranks
        recommendations = []
        for i, sim in enumerate(sorted_sims):
            sim['rank'] = i + 1
            recommendations.append(SimilarMovieRecommendation(**sim))
        
        logger.info(f"Returning {len(recommendations)} similar movies")
        
        return SimilarMoviesResponse(
            input_movies=[InputMovie(movie_id=m['movie_id'], movie_title=m['title']) for m in targets],
            strategy=strategy,
            similar_movies=recommendations,
            total_count=len(recommendations)
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in similar movies: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get("/reddit-buzz", response_model=RedditBuzzResponse)
async def get_reddit_buzz_recommendations(
    genre: Optional[str] = Query(None, description="Filter by genre"),
    days_back: int = Query(7, ge=1, le=30, description="Days of Reddit data to analyze"),
    limit: int = Query(20, ge=1, le=100, description="Maximum number of results"),
    queries: MovieQueries = Depends(get_movie_queries)
):
    """
    Get pure Reddit buzz rankings (Reddit component only)
    
    Formula: Reddit_Score = weighted_engagement * recency_decay * volume_multiplier
    - weighted_engagement: upvotes + comments*2 + awards*10
    - recency_decay: exp(-age_hours / 24) with 24h half-life
    - volume_multiplier: 1 + log10(post_count + 1) / 10
    """
    try:
        logger.info(f"Fetching Reddit buzz recommendations: genre={genre}, days_back={days_back}, limit={limit}")
        
        # Get Reddit data from speed layer
        reddit_data = queries.get_reddit_buzz_data(
            genre=genre,
            days_back=days_back
        )
        
        if not reddit_data:
            return RedditBuzzResponse(
                recommendations=[],
                total_count=0,
                filters_applied={
                    "genre": genre,
                    "days_back": days_back,
                    "limit": limit
                }
            )
        
        logger.info(f"Retrieved {len(reddit_data)} movies with Reddit data")
        
        # Calculate Reddit buzz scores
        now = datetime.utcnow()
        reddit_rankings = []
        
        for movie_data in reddit_data:
            # Calculate weighted engagement
            W = (
                movie_data.get('total_upvotes', 0) +
                movie_data.get('total_comments', 0) * 2 +
                movie_data.get('total_awards', 0) * 10
            )
            
            # Skip if no engagement
            if W == 0:
                continue
            
            # Calculate recency decay (exponential with 24h half-life)
            last_window = movie_data.get('last_window_start')
            if last_window:
                age_hours = (now - last_window).total_seconds() / 3600
            else:
                age_hours = days_back * 24  # Assume oldest
            
            decay = math.exp(-age_hours / 24)
            
            # Calculate volume multiplier (log scale to prevent over-weighting)
            post_count = movie_data.get('post_count', 1)
            multiplier = 1 + math.log10(post_count + 1) / 10
            
            # Final Reddit buzz score
            reddit_score = W * decay * multiplier
            
            reddit_rankings.append({
                'movie_title': movie_data['movie_title'],
                'reddit_buzz_score': round(reddit_score, 1),
                'total_engagement': W,
                'post_count': post_count,
                'total_comments': movie_data.get('total_comments', 0),
                'hours_since_last_window': round(age_hours, 1),
                'viral_score': movie_data.get('viral_score', 0)
            })
        
        # Sort by Reddit buzz score
        sorted_rankings = sorted(reddit_rankings, key=lambda x: x['reddit_buzz_score'], reverse=True)
        
        # Get genre info from batch layer for top results
        top_rankings = sorted_rankings[:limit]
        movie_titles = [r['movie_title'] for r in top_rankings]
        
        # Fetch movie_id and genre from batch layer
        batch_movies = {
            m['title']: m 
            for m in queries.movie_intelligence.find(
                {'title': {'$in': movie_titles}},
                {'movie_id': 1, 'title': 1, 'genre': 1, 'genres': 1, '_id': 0}
            )
        }
        
        # Build final recommendations
        recommendations = []
        for i, ranking in enumerate(top_rankings):
            title = ranking['movie_title']
            batch_movie = batch_movies.get(title, {})
            
            # Get genre (handle both flat and array)
            genre_value = batch_movie.get('genre')
            if not genre_value and 'genres' in batch_movie:
                genres = batch_movie.get('genres', [])
                genre_value = genres[0] if genres else None
            
            recommendations.append(RedditBuzzRecommendation(
                rank=i + 1,
                movie_id=batch_movie.get('movie_id', 0),
                movie_title=title,
                genre=genre_value,
                reddit_buzz_score=ranking['reddit_buzz_score'],
                total_engagement=ranking['total_engagement'],
                reddit_mentions=ranking['post_count']
            ))
        
        logger.info(f"Returning {len(recommendations)} Reddit buzz recommendations")
        
        return RedditBuzzResponse(
            recommendations=recommendations,
            total_count=len(recommendations),
            filters_applied={
                "genre": genre,
                "days_back": days_back,
                "limit": limit
            }
        )
    
    except Exception as e:
        logger.error(f"Error in Reddit buzz recommendations: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@router.get("/tmdb-quality", response_model=TMDBQualityResponse)
async def get_tmdb_quality_recommendations(
    genre: Optional[str] = Query(None, description="Filter by genre"),
    min_vote_count: int = Query(100, ge=1, description="Minimum vote count threshold"),
    limit: int = Query(10, ge=1, le=100, description="Maximum number of recommendations"),
    queries: MovieQueries = Depends(get_movie_queries)
) -> TMDBQualityResponse:
    """
    Get TMDB quality rankings based on Bayesian average, popularity, and freshness.
    
    **TMDB Quality Score Formula:**
    ```
    tmdb_quality_score = weighted_rating * (0.7 + 0.3 * popularity_factor) * freshness_bonus
    
    Where:
    - weighted_rating (Bayesian average): WR = (v/(v+m))*R + (m/(v+m))*C
      - v = vote_count for the movie
      - m = minimum votes threshold (100)
      - R = vote_average for the movie
      - C = mean vote_average across all movies
    
    - popularity_factor: P = log10(vote_count + 1) / 6
      - Normalized to [0, 1]
      - Assumption: max vote_count ≈ 1M (log10(1M) ≈ 6)
    
    - freshness_bonus:
      - 1.1 if released in last 6 months
      - 1.05 if released in last 1 year
      - 1.0 otherwise
    ```
    
    Args:
        genre: Optional genre filter
        min_vote_count: Minimum vote count threshold (default 100)
        limit: Maximum results (default 10, max 100)
        
    Returns:
        TMDBQualityResponse with ranked movies
    """
    try:
        logger.info(f"TMDB quality request: genre={genre}, min_vote_count={min_vote_count}, limit={limit}")
        
        # Get mean vote average across dataset (C parameter for Bayesian average)
        C = queries.get_mean_vote_average()
        m = min_vote_count
        
        logger.info(f"Bayesian parameters: C={C:.2f}, m={m}")
        
        # Get candidate movies
        movies = queries.get_tmdb_quality_data(min_vote_count=m, genre=genre)
        
        if not movies:
            logger.warning(f"No movies found with min_vote_count >= {m}")
            return TMDBQualityResponse(
                recommendations=[],
                total_count=0,
                filters_applied={
                    "genre": genre,
                    "min_vote_count": m,
                    "limit": limit
                }
            )
        
        logger.info(f"Found {len(movies)} candidate movies")
        
        # Calculate TMDB quality scores
        rankings = []
        now = datetime.utcnow()
        
        for movie in movies:
            v = movie.get('vote_count', 0)
            R = movie.get('vote_average', 0)
            
            if v == 0:
                continue
            
            # Weighted rating (Bayesian average)
            WR = (v / (v + m)) * R + (m / (v + m)) * C
            
            # Popularity factor (log scale, normalized to 0-1)
            P = math.log10(v + 1) / 6.0
            P = min(P, 1.0)  # Cap at 1.0
            
            # Freshness bonus
            freshness_bonus = 1.0
            release_date_str = movie.get('release_date')
            if release_date_str:
                try:
                    if isinstance(release_date_str, str):
                        release_date = datetime.strptime(release_date_str, '%Y-%m-%d')
                    else:
                        release_date = release_date_str
                    
                    months_since_release = (now - release_date).days / 30.0
                    
                    if months_since_release <= 6:
                        freshness_bonus = 1.1
                    elif months_since_release <= 12:
                        freshness_bonus = 1.05
                except (ValueError, TypeError) as e:
                    logger.debug(f"Could not parse release_date for movie {movie.get('title')}: {e}")
            
            # Final TMDB quality score
            tmdb_quality_score = WR * (0.7 + 0.3 * P) * freshness_bonus
            
            # Get genre (handle both flat and array)
            genre_value = movie.get('genre')
            if not genre_value and 'genres' in movie:
                genres = movie.get('genres', [])
                genre_value = genres[0] if genres else None
            
            rankings.append({
                'movie_id': movie.get('movie_id', 0),
                'movie_title': movie.get('title', 'Unknown'),
                'genre': genre_value,
                'tmdb_quality_score': round(tmdb_quality_score, 2),
                'vote_average': R,
                'vote_count': v,
                'weighted_rating': round(WR, 2),
                'popularity_factor': round(P, 3),
                'release_date': release_date_str
            })
        
        # Sort by TMDB quality score (descending)
        sorted_rankings = sorted(rankings, key=lambda x: x['tmdb_quality_score'], reverse=True)
        
        # Apply limit and create response objects
        from api.schemas.recommendations import TMDBQualityRecommendation
        recommendations = []
        for i, ranking in enumerate(sorted_rankings[:limit]):
            recommendations.append(TMDBQualityRecommendation(
                rank=i + 1,
                **ranking
            ))
        
        logger.info(f"Returning {len(recommendations)} TMDB quality recommendations")
        
        return TMDBQualityResponse(
            recommendations=recommendations,
            total_count=len(recommendations),
            filters_applied={
                "genre": genre,
                "min_vote_count": m,
                "limit": limit
            }
        )
    
    except Exception as e:
        logger.error(f"Error in TMDB quality recommendations: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get("/personalized")
async def get_personalized_recommendations():
    """Get personalized recommendations (future feature)"""
    raise HTTPException(status_code=501, detail="Not implemented yet")

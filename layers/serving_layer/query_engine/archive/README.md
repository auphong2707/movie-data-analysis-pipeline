# Archived Query Engine Components

**Archive Date:** December 28, 2025

These components were part of the original serving layer architecture but have been replaced by simpler, inline implementations in the current API routes.

## Archived Components

### 1. `view_merger.py` (1,322 lines)
**Original Purpose:** Merge batch and speed layer data using 48-hour cutoff strategy

**Why Archived:**
- Current API routes implement merging logic **inline** directly in endpoint handlers
- Simplified architecture: direct MongoDB queries instead of abstraction layers
- Better visibility: business logic is now in the same file as the API endpoint

**Replacement:** Inline merging in:
- `api/routes/crisis_detection.py` (lines 135-170)
- `api/routes/viral_detection.py` (lines 85-115)
- `api/routes/recommendations.py` (lines 125-190)

---

### 2. `query_router.py` (345 lines)
**Original Purpose:** Route queries to batch vs speed layer based on timestamp and freshness

**Why Archived:**
- API routes now query MongoDB collections **directly**
- No need for routing abstraction - endpoints know which collections to query
- Simpler data flow: endpoint → MongoDB → response

**Replacement:** Direct MongoDB queries using `MovieQueries` helper class

---

### 3. `recommendation_engine.py` (526 lines)
**Original Purpose:** Content-based filtering with trending boost and sentiment re-ranking

**Why Archived:**
- All recommendation logic moved to `api/routes/recommendations.py`
- Business logic is now **inline** in endpoint handlers
- More maintainable: formula changes don't require updating abstraction layer

**Replacement:**
- `api/routes/recommendations.py` (750 lines) contains all recommendation endpoints
- Uses `SimilarityEngine` for cosine similarity calculations only

---

### 4. `cache_manager.py` (368 lines)
**Original Purpose:** Redis-based caching for API responses

**Why Archived:**
- Defined but **not actively used** in current API routes
- No `@cache` decorators found in current endpoints
- Redis infrastructure exists but caching is not implemented

**Status:** Could be reactivated if caching is needed (Redis container still runs)

---

## Active Components

Only these components remain active in the `query_engine/` directory:

1. **`similarity_engine.py`** ✅
   - Used by `api/routes/recommendations.py` for content-based similarity
   - Functions: `build_feature_vector()`, `calculate_similarity_score()`

2. **`utils.py`** ✅
   - Utility functions for text processing (fuzzy matching, normalization)

---

## Architecture Evolution

### Old Architecture (Archived)
```
API Endpoint
    ↓
QueryRouter (decides batch vs speed)
    ↓
ViewMerger (merges data)
    ↓
RecommendationEngine (business logic)
    ↓
CacheManager (caches result)
    ↓
Response
```

### New Architecture (Current)
```
API Endpoint
    ├─ Direct MongoDB queries
    ├─ Inline business logic
    ├─ SimilarityEngine (for similar movies only)
    └─ Response
```

**Benefits:**
- Less abstraction = easier to understand
- Business logic visible in endpoint = easier to debug
- Fewer layers = faster development
- Direct queries = better performance visibility

---

## Restoration Guide

If you need to restore these components:

1. Move files from `archive/` back to `query_engine/`
2. Update `query_engine/__init__.py` to export them
3. Update API routes to use abstraction layers instead of inline logic

**Note:** Consider whether the abstraction is truly needed. The current inline approach has proven simpler and more maintainable.

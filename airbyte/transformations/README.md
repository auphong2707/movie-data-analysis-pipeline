# Airbyte Data Transformations

This directory contains SQL-based transformations and dbt models for processing movie data.

## Transformation Types

### 1. Data Cleaning
- Remove null/invalid records
- Standardize date formats
- Clean text fields (remove HTML, special characters)
- Validate numeric fields

### 2. Data Enrichment
- Add calculated fields (age ratings, popularity scores)
- Genre mapping and normalization
- Language code standardization
- Currency conversions

### 3. Data Aggregation
- Movie statistics by genre/year/rating
- Actor/Director filmography summaries
- Review sentiment aggregations
- Box office performance metrics

### 4. Data Quality Checks
- Duplicate detection and removal
- Data completeness validation
- Format consistency checks
- Business rule validations

## Files

- `models/` - dbt transformation models
- `macros/` - Reusable transformation functions
- `tests/` - Data quality tests
- `profiles.yml` - dbt connection profiles
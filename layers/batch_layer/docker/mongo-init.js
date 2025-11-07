// ========================================
// MongoDB Initialization Script
// ========================================
// Creates batch_views collection with indexes
// ========================================

print('========================================');
print('MongoDB Initialization Script');
print('========================================');

// Switch to moviedb database
db = db.getSiblingDB('moviedb');

// Create batch_views collection if it doesn't exist
if (!db.getCollectionNames().includes('batch_views')) {
    db.createCollection('batch_views');
    print('Created batch_views collection');
} else {
    print('batch_views collection already exists');
}

// Drop existing indexes (except _id)
db.batch_views.dropIndexes();
print('Dropped existing indexes');

// Create indexes for batch_views collection
print('Creating indexes for batch_views collection...');

// Index 1: Compound index on view_type + genre + year + month
db.batch_views.createIndex(
    { view_type: 1, genre: 1, year: -1, month: -1 },
    { name: 'idx_view_genre_time', background: true }
);
print('Created index: idx_view_genre_time');

// Index 2: Compound index on view_type + movie_id
db.batch_views.createIndex(
    { view_type: 1, movie_id: 1 },
    { name: 'idx_view_movie', background: true }
);
print('Created index: idx_view_movie');

// Index 3: Compound index on view_type + computed_at (for latest data retrieval)
db.batch_views.createIndex(
    { view_type: 1, computed_at: -1 },
    { name: 'idx_view_computed', background: true }
);
print('Created index: idx_view_computed');

// Index 4: Index on batch_run_id for tracking
db.batch_views.createIndex(
    { batch_run_id: 1 },
    { name: 'idx_batch_run', background: true }
);
print('Created index: idx_batch_run');

// Index 5: Compound index for trending queries
db.batch_views.createIndex(
    { view_type: 1, window_days: 1, trending_score: -1 },
    { name: 'idx_trending', background: true }
);
print('Created index: idx_trending');

// List all indexes
print('Current indexes on batch_views:');
db.batch_views.getIndexes().forEach(function(idx) {
    print('  - ' + idx.name + ': ' + JSON.stringify(idx.key));
});

// Create a sample document to validate schema
const sampleDoc = {
    view_type: 'genre_analytics',
    genre: 'Action',
    year: 2025,
    month: 1,
    total_movies: 0,
    avg_rating: 0.0,
    avg_revenue: 0.0,
    top_movies: [],
    computed_at: new Date(),
    batch_run_id: 'init_' + new Date().toISOString()
};

// Insert sample document and remove it (to validate schema)
db.batch_views.insertOne(sampleDoc);
db.batch_views.deleteOne({ batch_run_id: sampleDoc.batch_run_id });
print('Schema validation successful');

print('========================================');
print('MongoDB initialization completed!');
print('Collection: moviedb.batch_views');
print('Indexes: 6 (including _id)');
print('========================================');

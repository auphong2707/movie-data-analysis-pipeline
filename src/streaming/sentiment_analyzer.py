"""
Sentiment analysis using Spark NLP for movie reviews.
"""
import logging
from typing import Dict, Any, Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml import Pipeline
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml.classification import LogisticRegression, NaiveBayes
from pyspark.ml.evaluation import BinaryClassificationEvaluator

logger = logging.getLogger(__name__)

class SentimentAnalyzer:
    """Sentiment analysis for movie reviews using Spark ML."""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.model = None
        self.is_trained = False
        
    def create_basic_sentiment_pipeline(self):
        """Create a basic sentiment analysis pipeline using Spark ML."""
        logger.info("Creating basic sentiment analysis pipeline...")
        
        # Tokenization
        tokenizer = Tokenizer(inputCol="content", outputCol="words")
        
        # Remove stop words
        stop_words_remover = StopWordsRemover(
            inputCol="words", 
            outputCol="filtered_words",
            stopWords=StopWordsRemover.loadDefaultStopWords("english")
        )
        
        # Feature extraction
        hashing_tf = HashingTF(
            inputCol="filtered_words", 
            outputCol="raw_features", 
            numFeatures=10000
        )
        
        idf = IDF(inputCol="raw_features", outputCol="features")
        
        # Classifier
        lr = LogisticRegression(
            featuresCol="features",
            labelCol="sentiment_label",
            maxIter=100,
            regParam=0.01
        )
        
        # Create pipeline
        pipeline = Pipeline(stages=[
            tokenizer,
            stop_words_remover, 
            hashing_tf,
            idf,
            lr
        ])
        
        return pipeline
    
    def analyze_sentiment_basic(self, df: DataFrame) -> DataFrame:
        """Apply basic rule-based sentiment analysis."""
        logger.info("Applying basic sentiment analysis...")
        
        # Define sentiment keywords
        positive_words = ["excellent", "amazing", "fantastic", "wonderful", "great", "love", 
                         "perfect", "brilliant", "outstanding", "superb", "awesome", "incredible"]
        negative_words = ["terrible", "awful", "horrible", "hate", "worst", "bad", "stupid",
                         "boring", "disappointing", "waste", "poor", "pathetic"]
        
        # Create sentiment scoring UDF
        def calculate_sentiment_score(text: str) -> float:
            if not text:
                return 0.0
            
            text_lower = text.lower()
            positive_count = sum(1 for word in positive_words if word in text_lower)
            negative_count = sum(1 for word in negative_words if word in text_lower)
            
            # Simple scoring: positive - negative, normalized by text length
            word_count = len(text.split())
            if word_count == 0:
                return 0.0
            
            score = (positive_count - negative_count) / max(word_count / 100, 1)
            return max(-1.0, min(1.0, score))  # Clamp between -1 and 1
        
        sentiment_udf = udf(calculate_sentiment_score, DoubleType())
        
        # Apply sentiment analysis
        df_with_sentiment = df.withColumn(
            "sentiment_score", sentiment_udf(col("content"))
        ).withColumn(
            "sentiment_label",
            when(col("sentiment_score") > 0.1, "positive")
            .when(col("sentiment_score") < -0.1, "negative")
            .otherwise("neutral")
        ).withColumn(
            "sentiment_confidence",
            abs(col("sentiment_score"))
        )
        
        return df_with_sentiment
    
    def analyze_sentiment_advanced(self, df: DataFrame) -> DataFrame:
        """Apply advanced sentiment analysis (placeholder for Spark NLP)."""
        logger.info("Applying advanced sentiment analysis...")
        
        # This would use Spark NLP in a real implementation
        # For now, we'll use the basic approach with some enhancements
        
        # Language detection (simplified)
        df_with_lang = df.withColumn(
            "detected_language",
            when(col("content").rlike(r"[àáâãäåæçèéêëìíîïðñòóôõöøùúûüýþÿ]"), "non-english")
            .otherwise("english")
        )
        
        # Apply sentiment analysis only to English reviews
        df_with_sentiment = self.analyze_sentiment_basic(df_with_lang)
        
        # Add language-specific adjustments
        df_final = df_with_sentiment.withColumn(
            "final_sentiment_score",
            when(col("detected_language") == "english", col("sentiment_score"))
            .otherwise(lit(0.0))  # Neutral for non-English reviews
        ).withColumn(
            "final_sentiment_label",
            when(col("detected_language") == "english", col("sentiment_label"))
            .otherwise(lit("neutral"))
        )
        
        return df_final
    
    def extract_sentiment_features(self, df: DataFrame) -> DataFrame:
        """Extract additional sentiment-related features."""
        logger.info("Extracting sentiment features...")
        
        # Text statistics
        df_features = df.withColumn(
            "exclamation_count", size(split(col("content"), "!")) - 1
        ).withColumn(
            "question_count", size(split(col("content"), r"\?")) - 1
        ).withColumn(
            "uppercase_ratio", 
            length(regexp_replace(col("content"), "[^A-Z]", "")) / length(col("content"))
        ).withColumn(
            "has_profanity",
            col("content").rlike(r"(?i)\b(damn|hell|shit|fuck|crap)\b")
        ).withColumn(
            "review_intensity",
            when(col("exclamation_count") > 3, "high")
            .when(col("exclamation_count") > 1, "medium")
            .otherwise("low")
        )
        
        return df_features
    
    def aggregate_movie_sentiment(self, df: DataFrame) -> DataFrame:
        """Aggregate sentiment scores by movie."""
        logger.info("Aggregating sentiment by movie...")
        
        movie_sentiment = df.groupBy("movie_id").agg(
            count("*").alias("total_reviews"),
            avg("sentiment_score").alias("avg_sentiment_score"),
            stddev("sentiment_score").alias("sentiment_score_stddev"),
            sum(when(col("sentiment_label") == "positive", 1).otherwise(0)).alias("positive_reviews"),
            sum(when(col("sentiment_label") == "negative", 1).otherwise(0)).alias("negative_reviews"),
            sum(when(col("sentiment_label") == "neutral", 1).otherwise(0)).alias("neutral_reviews"),
            max("sentiment_score").alias("max_sentiment_score"),
            min("sentiment_score").alias("min_sentiment_score"),
            avg("sentiment_confidence").alias("avg_confidence")
        ).withColumn(
            "positive_ratio", col("positive_reviews") / col("total_reviews")
        ).withColumn(
            "negative_ratio", col("negative_reviews") / col("total_reviews")
        ).withColumn(
            "neutral_ratio", col("neutral_reviews") / col("total_reviews")
        ).withColumn(
            "sentiment_consensus",
            when(col("positive_ratio") > 0.6, "positive")
            .when(col("negative_ratio") > 0.6, "negative")
            .when(col("positive_ratio") > col("negative_ratio"), "mixed_positive")
            .when(col("negative_ratio") > col("positive_ratio"), "mixed_negative")
            .otherwise("neutral")
        ).withColumn(
            "sentiment_polarization",
            (col("positive_ratio") + col("negative_ratio")) - col("neutral_ratio")
        )
        
        return movie_sentiment

class LanguageDetector:
    """Simple language detection for text processing."""
    
    @staticmethod
    def detect_language_udf():
        """UDF for basic language detection."""
        def detect_lang(text: str) -> str:
            if not text:
                return "unknown"
            
            text_lower = text.lower()
            
            # Simple heuristics for language detection
            # Vietnamese patterns
            if any(char in text_lower for char in "àáạảãâầấậẩẫăằắặẳẵèéẹẻẽêềếệểễìíịỉĩòóọỏõôồốộổỗơờớợởỡùúụủũưừứựửữỳýỵỷỹđ"):
                return "vietnamese"
            
            # French patterns
            if any(char in text_lower for char in "àâäæéèêëïîôùûüÿç"):
                return "french"
            
            # Spanish patterns  
            if any(char in text_lower for char in "ñáéíóúü"):
                return "spanish"
            
            # German patterns
            if any(char in text_lower for char in "äöüß"):
                return "german"
            
            # Default to English if ASCII characters
            return "english"
        
        return udf(detect_lang, StringType())
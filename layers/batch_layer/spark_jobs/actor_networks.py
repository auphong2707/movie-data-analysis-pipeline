"""
Actor Networks Analysis Job using GraphFrames/GraphX.

This PySpark job builds co-appearance networks from actor data in the Silver layer,
calculating collaboration metrics and network centrality measures.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, isnan, isnull, sum as spark_sum, count, avg, max as spark_max,
    min as spark_min, collect_list, collect_set, size, desc, asc,
    explode, posexplode, struct, array, lit, coalesce, concat, concat_ws,
    year, month, date_format, unix_timestamp, from_unixtime,
    row_number, rank, dense_rank, ntile, greatest, least
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
    LongType, DateType, TimestampType, ArrayType
)
from pyspark.sql.window import Window
import yaml

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ActorNetworksAnalyzer:
    """Analyzes actor collaboration networks from movie cast data."""
    
    def __init__(self, spark: SparkSession, config: Dict):
        """
        Initialize actor networks analyzer.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.hdfs_config = config.get("hdfs", {})
        self.job_config = config.get("jobs", {}).get("actor_networks", {})
        
        # Set up paths
        self.silver_path = self.hdfs_config["paths"]["silver"]
        self.gold_path = self.hdfs_config["paths"]["gold"]
        self.errors_path = self.hdfs_config["paths"]["errors"]
        
        # Performance settings
        self.graph_checkpoint_interval = self.job_config.get("graph_checkpoint_interval", 20)
        self.max_iterations = self.job_config.get("max_iterations", 10)
        self.tolerance = self.job_config.get("tolerance", 1e-6)
        
        # Network analysis parameters
        self.min_collaboration_threshold = 2  # Minimum movies together
        self.recency_decay_factor = 0.9  # Decay factor for older collaborations
        self.top_actors_limit = 1000  # Limit for network analysis
        
        # Configure Spark for graph processing
        self._configure_spark_for_graphs()
    
    def _configure_spark_for_graphs(self):
        """Configure Spark session for optimal graph processing."""
        self.spark.conf.set("spark.sql.adaptive.enabled", "true")
        self.spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        self.spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        self.spark.conf.set("spark.sql.shuffle.partitions", "200")
    
    def build_actor_networks(
        self, 
        start_time: datetime,
        end_time: datetime,
        batch_id: str
    ) -> str:
        """
        Build actor collaboration networks from Silver layer data.
        
        Args:
            start_time: Start time for data processing window
            end_time: End time for data processing window
            batch_id: Unique identifier for this batch
            
        Returns:
            str: Path where actor networks data was written
        """
        logger.info(f"Building actor networks for batch {batch_id}")
        
        try:
            # Load Silver data with cast information
            silver_df = self._load_silver_cast_data(start_time, end_time)
            
            if silver_df.count() == 0:
                logger.warning("No Silver cast data found for the specified time range")
                return ""
            
            # Extract actor collaborations
            collaborations_df = self._extract_actor_collaborations(silver_df)
            
            # Calculate collaboration metrics
            collaboration_metrics_df = self._calculate_collaboration_metrics(collaborations_df)
            
            # Build network edges
            network_edges_df = self._build_network_edges(collaboration_metrics_df)
            
            # Calculate network centrality (simplified approach)
            actor_centrality_df = self._calculate_actor_centrality(network_edges_df)
            
            # Create final actor networks view
            actor_networks_df = self._create_actor_networks_view(
                network_edges_df, 
                actor_centrality_df,
                batch_id
            )
            
            # Write to Gold layer
            networks_path = self._write_actor_networks(actor_networks_df, batch_id)
            
            logger.info(f"Actor networks analysis completed: {networks_path}")
            return networks_path
            
        except Exception as e:
            logger.error(f"Error in actor networks analysis: {e}")
            raise
    
    def _load_silver_cast_data(self, start_time: datetime, end_time: datetime) -> DataFrame:
        """
        Load Silver layer data with cast information.
        
        Args:
            start_time: Start of time window
            end_time: End of time window
            
        Returns:
            DataFrame: Silver data with cast information
        """
        logger.info("Loading Silver cast data")
        
        try:
            cast_df = (self.spark.read
                      .parquet(self.silver_path)
                      .filter(
                          (col("processed_timestamp") >= start_time) &
                          (col("processed_timestamp") <= end_time) &
                          (col("cast").isNotNull()) &
                          (size(col("cast")) > 0) &
                          (col("quality_flag").isin(["OK", "WARNING"]))
                      )
                      .select(
                          col("movie_id"),
                          col("title"),
                          col("release_date"),
                          col("cast"),
                          col("vote_average"),
                          col("popularity"),
                          col("budget"),
                          col("revenue")
                      ))
            
            logger.info(f"Loaded {cast_df.count()} movies with cast data")
            return cast_df
            
        except Exception as e:
            logger.error(f"Error loading Silver cast data: {e}")
            raise
    
    def _extract_actor_collaborations(self, silver_df: DataFrame) -> DataFrame:
        """
        Extract actor-to-actor collaborations from cast data.
        
        Args:
            silver_df: Silver layer data with cast
            
        Returns:
            DataFrame: Actor collaborations data
        """
        logger.info("Extracting actor collaborations")
        
        try:
            # Explode cast to get individual actor records
            actors_df = (silver_df
                        .select(
                            col("movie_id"),
                            col("title"), 
                            col("release_date"),
                            col("vote_average"),
                            col("popularity"),
                            explode(col("cast")).alias("actor")
                        )
                        .select(
                            col("movie_id"),
                            col("title"),
                            col("release_date"),
                            col("vote_average"),
                            col("popularity"),
                            col("actor.name").alias("actor_name"),
                            col("actor.character").alias("character"),
                            coalesce(col("actor.order"), lit(999)).alias("cast_order")
                        )
                        .filter(col("actor_name").isNotNull())
                        .filter(col("actor_name") != ""))
            
            # Self-join to find co-appearances (collaborations)
            collaborations_df = (actors_df.alias("a1")
                               .join(
                                   actors_df.alias("a2"),
                                   col("a1.movie_id") == col("a2.movie_id"),
                                   "inner"
                               )
                               .filter(col("a1.actor_name") < col("a2.actor_name"))  # Avoid duplicates
                               .select(
                                   col("a1.actor_name").alias("actor1"),
                                   col("a2.actor_name").alias("actor2"),
                                   col("a1.movie_id"),
                                   col("a1.title"),
                                   col("a1.release_date"),
                                   col("a1.vote_average"),
                                   col("a1.popularity"),
                                   col("a1.cast_order").alias("actor1_order"),
                                   col("a2.cast_order").alias("actor2_order")
                               ))
            
            logger.info(f"Extracted {collaborations_df.count()} actor collaborations")
            return collaborations_df
            
        except Exception as e:
            logger.error(f"Error extracting actor collaborations: {e}")
            raise
    
    def _calculate_collaboration_metrics(self, collaborations_df: DataFrame) -> DataFrame:
        """
        Calculate collaboration metrics between actors.
        
        Args:
            collaborations_df: Raw collaborations data
            
        Returns:
            DataFrame: Collaborations with metrics
        """
        logger.info("Calculating collaboration metrics")
        
        try:
            # Calculate recency weights (more recent collaborations have higher weight)
            current_date = datetime.now()
            
            weighted_collaborations = (collaborations_df
                                     .withColumn("years_ago",
                                               (unix_timestamp(lit(current_date)) - 
                                                unix_timestamp(col("release_date"))) / (365 * 24 * 3600))
                                     .withColumn("recency_weight",
                                               pow(lit(self.recency_decay_factor), col("years_ago")))
                                     .withColumn("quality_weight",
                                               (coalesce(col("vote_average"), lit(5.0)) / 10.0) * 0.5 +
                                               (coalesce(col("popularity"), lit(1.0)) / 100.0) * 0.5)
                                     .withColumn("collaboration_weight",
                                               col("recency_weight") * col("quality_weight")))
            
            # Aggregate collaboration metrics by actor pair
            collaboration_metrics = (weighted_collaborations
                                   .groupBy("actor1", "actor2")
                                   .agg(
                                       count("movie_id").alias("collaboration_count"),
                                       spark_sum("collaboration_weight").alias("collaboration_score"),
                                       max("release_date").alias("recent_collaboration_date"),
                                       collect_list("title").alias("shared_movies"),
                                       avg("vote_average").alias("avg_movie_rating"),
                                       avg("popularity").alias("avg_movie_popularity")
                                   )
                                   .filter(col("collaboration_count") >= self.min_collaboration_threshold))
            
            logger.info(f"Calculated metrics for {collaboration_metrics.count()} actor pairs")
            return collaboration_metrics
            
        except Exception as e:
            logger.error(f"Error calculating collaboration metrics: {e}")
            raise
    
    def _build_network_edges(self, collaboration_metrics_df: DataFrame) -> DataFrame:
        """
        Build network edges from collaboration metrics.
        
        Args:
            collaboration_metrics_df: Collaboration metrics data
            
        Returns:
            DataFrame: Network edges with weights
        """
        logger.info("Building network edges")
        
        try:
            # Create bidirectional edges for the network
            edges_df = (collaboration_metrics_df
                       .select(
                           col("actor1").alias("src"),
                           col("actor2").alias("dst"),
                           col("collaboration_count"),
                           col("collaboration_score"),
                           col("recent_collaboration_date"),
                           col("shared_movies"),
                           col("avg_movie_rating"),
                           col("avg_movie_popularity")
                       )
                       .union(
                           collaboration_metrics_df.select(
                               col("actor2").alias("src"),
                               col("actor1").alias("dst"),
                               col("collaboration_count"),
                               col("collaboration_score"),
                               col("recent_collaboration_date"),
                               col("shared_movies"),
                               col("avg_movie_rating"),
                               col("avg_movie_popularity")
                           )
                       ))
            
            logger.info(f"Built {edges_df.count()} network edges")
            return edges_df
            
        except Exception as e:
            logger.error(f"Error building network edges: {e}")
            raise
    
    def _calculate_actor_centrality(self, network_edges_df: DataFrame) -> DataFrame:
        """
        Calculate network centrality measures for actors.
        
        Args:
            network_edges_df: Network edges data
            
        Returns:
            DataFrame: Actor centrality measures
        """
        logger.info("Calculating actor centrality measures")
        
        try:
            # Calculate degree centrality (number of connections)
            degree_centrality = (network_edges_df
                               .groupBy("src")
                               .agg(
                                   count("dst").alias("degree"),
                                   spark_sum("collaboration_score").alias("weighted_degree"),
                                   avg("collaboration_score").alias("avg_collaboration_strength")
                               )
                               .withColumnRenamed("src", "actor"))
            
            # Calculate eigenvector centrality approximation using iterative approach
            # Start with uniform centrality values
            vertices_df = (network_edges_df
                          .select("src")
                          .union(network_edges_df.select("dst"))
                          .distinct()
                          .withColumnRenamed("src", "actor")
                          .withColumn("centrality", lit(1.0)))
            
            # Simple PageRank-like iteration (simplified eigenvector centrality)
            for iteration in range(min(self.max_iterations, 5)):  # Limit iterations for performance
                logger.info(f"Centrality iteration {iteration + 1}")
                
                # Calculate new centrality scores
                edge_contributions = (network_edges_df
                                    .join(vertices_df, network_edges_df.src == vertices_df.actor)
                                    .select(
                                        col("dst").alias("target_actor"),
                                        (col("centrality") * col("collaboration_score")).alias("contribution")
                                    ))
                
                new_centrality = (edge_contributions
                                .groupBy("target_actor")
                                .agg(spark_sum("contribution").alias("new_centrality"))
                                .withColumnRenamed("target_actor", "actor"))
                
                # Normalize centrality scores
                max_centrality = new_centrality.agg(spark_max("new_centrality")).collect()[0][0]
                if max_centrality and max_centrality > 0:
                    new_centrality = new_centrality.withColumn(
                        "new_centrality",
                        col("new_centrality") / max_centrality
                    )
                
                # Update vertices with new centrality
                vertices_df = (vertices_df
                              .join(new_centrality, "actor", "left")
                              .withColumn("centrality", 
                                        coalesce(col("new_centrality"), col("centrality")))
                              .drop("new_centrality"))
            
            # Combine degree and eigenvector centrality
            final_centrality = (degree_centrality
                               .join(vertices_df, "actor", "inner")
                               .withColumn("network_centrality",
                                         (col("centrality") * 0.7) + 
                                         ((col("degree") / 100.0) * 0.3)))
            
            logger.info(f"Calculated centrality for {final_centrality.count()} actors")
            return final_centrality
            
        except Exception as e:
            logger.error(f"Error calculating actor centrality: {e}")
            # Return simplified centrality based on degree only
            return (network_edges_df
                   .groupBy("src")
                   .agg(count("dst").alias("degree"))
                   .withColumnRenamed("src", "actor")
                   .withColumn("network_centrality", col("degree") / 100.0))
    
    def _create_actor_networks_view(
        self, 
        network_edges_df: DataFrame,
        actor_centrality_df: DataFrame,
        batch_id: str
    ) -> DataFrame:
        """
        Create final actor networks view for Gold layer.
        
        Args:
            network_edges_df: Network edges data
            actor_centrality_df: Actor centrality data
            batch_id: Batch identifier
            
        Returns:
            DataFrame: Final actor networks view
        """
        logger.info("Creating final actor networks view")
        
        try:
            # Create monthly snapshot identifier
            snapshot_month = datetime.now().strftime("%Y-%m")
            
            # Join edges with centrality data
            actor_networks = (network_edges_df
                             .join(
                                 actor_centrality_df.select(
                                     col("actor").alias("src_actor"),
                                     col("network_centrality").alias("src_centrality")
                                 ),
                                 col("src") == col("src_actor"),
                                 "left"
                             )
                             .join(
                                 actor_centrality_df.select(
                                     col("actor").alias("dst_actor"),
                                     col("network_centrality").alias("dst_centrality")
                                 ),
                                 col("dst") == col("dst_actor"),
                                 "left"
                             )
                             .select(
                                 col("src").alias("actor_id"),
                                 col("src").alias("actor_name"),
                                 col("dst").alias("collaborator_id"),
                                 col("dst").alias("collaborator_name"),
                                 col("collaboration_count"),
                                 col("collaboration_score"),
                                 col("recent_collaboration_date"),
                                 col("shared_movies"),
                                 coalesce(col("src_centrality"), lit(0.0)).alias("network_centrality"),
                                 lit(snapshot_month).alias("snapshot_month"),
                                 lit(datetime.utcnow()).alias("computed_timestamp")
                             ))
            
            # Filter to keep only significant connections (top collaborations per actor)
            window_spec = (Window
                          .partitionBy("actor_id")
                          .orderBy(desc("collaboration_score")))
            
            filtered_networks = (actor_networks
                               .withColumn("collaboration_rank", row_number().over(window_spec))
                               .filter(col("collaboration_rank") <= 20)  # Top 20 collaborators per actor
                               .drop("collaboration_rank"))
            
            logger.info(f"Created actor networks view with {filtered_networks.count()} connections")
            return filtered_networks
            
        except Exception as e:
            logger.error(f"Error creating actor networks view: {e}")
            raise
    
    def _write_actor_networks(self, actor_networks_df: DataFrame, batch_id: str) -> str:
        """
        Write actor networks data to Gold layer.
        
        Args:
            actor_networks_df: Actor networks DataFrame
            batch_id: Batch identifier
            
        Returns:
            str: Path where data was written
        """
        logger.info("Writing actor networks to Gold layer")
        
        try:
            networks_path = f"{self.gold_path}/actor_networks"
            
            (actor_networks_df.write
             .mode("overwrite")
             .option("partitionOverwriteMode", "dynamic")
             .option("compression", "snappy")
             .partitionBy("snapshot_month")
             .parquet(networks_path))
            
            # Update metadata
            self._update_networks_metadata(networks_path, actor_networks_df.count(), batch_id)
            
            logger.info(f"Actor networks written to: {networks_path}")
            return networks_path
            
        except Exception as e:
            logger.error(f"Error writing actor networks: {e}")
            raise
    
    def _update_networks_metadata(self, networks_path: str, record_count: int, batch_id: str):
        """
        Update metadata for actor networks processing.
        
        Args:
            networks_path: Path where networks data was written
            record_count: Number of records written
            batch_id: Batch identifier
        """
        metadata_record = {
            "batch_id": batch_id,
            "job_type": "actor_networks",
            "networks_path": networks_path,
            "record_count": record_count,
            "processed_timestamp": datetime.utcnow().isoformat(),
            "status": "completed"
        }
        
        try:
            metadata_df = self.spark.createDataFrame([metadata_record])
            metadata_path = f"{self.hdfs_config['paths']['metadata']}/networks_log"
            
            (metadata_df.write
             .mode("append")
             .option("compression", "snappy")
             .json(metadata_path))
            
        except Exception as e:
            logger.error(f"Error updating networks metadata: {e}")


def load_config(config_path: str) -> Dict:
    """Load configuration from YAML file."""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


# CLI interface
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Actor Networks Analysis")
    parser.add_argument("--config", required=True, help="Path to configuration file")
    parser.add_argument("--start-time", required=True, help="Start time (ISO format)")
    parser.add_argument("--end-time", required=True, help="End time (ISO format)")
    parser.add_argument("--batch-id", required=True, help="Batch ID")
    
    args = parser.parse_args()
    
    # Load configuration
    config = load_config(args.config)
    
    # Parse times
    start_time = datetime.fromisoformat(args.start_time)
    end_time = datetime.fromisoformat(args.end_time)
    
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Actor_Networks_Analysis") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    try:
        # Initialize analyzer
        analyzer = ActorNetworksAnalyzer(spark, config)
        
        # Run networks analysis
        result_path = analyzer.build_actor_networks(
            start_time=start_time,
            end_time=end_time,
            batch_id=args.batch_id
        )
        
        logger.info(f"Actor networks analysis completed successfully: {result_path}")
        
    finally:
        spark.stop()
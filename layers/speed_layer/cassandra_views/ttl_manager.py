"""
TTL Manager for Cassandra Speed Layer Tables
Manages TTL settings, validation, and automatic adjustment.
"""

import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional, Tuple
from cassandra.cluster import Cluster, Session
from cassandra.query import SimpleStatement
import yaml

logger = logging.getLogger(__name__)


class TTLManager:
    """Manages Time-To-Live (TTL) settings for speed layer tables."""
    
    def __init__(self, session: Session, config_path: str = "config/cassandra_config.yaml"):
        """Initialize TTL Manager."""
        self.session = session
        
        # Load configuration
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        # Default TTL settings (in seconds)
        self.ttl_settings = {
            'review_sentiments': 172800,      # 48 hours
            'movie_stats': 172800,            # 48 hours  
            'movie_ratings_by_window': 172800, # 48 hours
            'trending_movies': 172800,        # 48 hours
            'breakout_movies': 172800,        # 48 hours
            'declining_movies': 172800,       # 48 hours
            'latest_movie_stats': 172800,     # 48 hours (materialized view)
            'top_trending_movies': 172800,    # 48 hours (materialized view)
            'recent_sentiments': 21600,       # 6 hours (materialized view)
            'movie_stats_hourly': 604800,     # 7 days (rollup table)
            'sentiment_hourly': 604800,       # 7 days (rollup table)
            'speed_layer_metrics': 259200     # 3 days (metrics table)
        }
        
        # Override with config if provided
        if 'ttl_settings' in self.config.get('cassandra', {}):
            self.ttl_settings.update(self.config['cassandra']['ttl_settings'])
        
        logger.info("TTL Manager initialized")
    
    def get_table_ttl(self, table_name: str) -> Optional[int]:
        """Get current TTL setting for a table."""
        
        query = """
            SELECT default_time_to_live
            FROM system_schema.tables  
            WHERE keyspace_name = %s AND table_name = %s
        """
        
        try:
            keyspace = self.config['cassandra']['keyspace']
            result = self.session.execute(query, [keyspace, table_name])
            row = result.one()
            
            if row:
                return row.default_time_to_live
            else:
                logger.warning(f"Table {table_name} not found")
                return None
                
        except Exception as e:
            logger.error(f"Failed to get TTL for table {table_name}: {e}")
            return None
    
    def set_table_ttl(self, table_name: str, ttl_seconds: int) -> bool:
        """Set TTL for a specific table."""
        
        query = f"""
            ALTER TABLE {table_name} 
            WITH default_time_to_live = {ttl_seconds}
        """
        
        try:
            self.session.execute(query)
            logger.info(f"Set TTL for table {table_name} to {ttl_seconds} seconds")
            return True
            
        except Exception as e:
            logger.error(f"Failed to set TTL for table {table_name}: {e}")
            return False
    
    def validate_all_ttls(self) -> Dict[str, Dict[str, Any]]:
        """Validate TTL settings for all speed layer tables."""
        
        validation_results = {}
        
        for table_name, expected_ttl in self.ttl_settings.items():
            try:
                current_ttl = self.get_table_ttl(table_name)
                
                validation_results[table_name] = {
                    'expected_ttl': expected_ttl,
                    'current_ttl': current_ttl,
                    'is_valid': current_ttl == expected_ttl,
                    'needs_update': current_ttl != expected_ttl
                }
                
                if current_ttl != expected_ttl:
                    logger.warning(f"TTL mismatch for {table_name}: "
                                 f"expected {expected_ttl}, got {current_ttl}")
                
            except Exception as e:
                validation_results[table_name] = {
                    'expected_ttl': expected_ttl,
                    'current_ttl': None,
                    'is_valid': False,
                    'needs_update': True,
                    'error': str(e)
                }
                logger.error(f"Failed to validate TTL for {table_name}: {e}")
        
        return validation_results
    
    def fix_all_ttls(self) -> Dict[str, bool]:
        """Fix TTL settings for all tables that need updates."""
        
        validation_results = self.validate_all_ttls()
        fix_results = {}
        
        for table_name, validation in validation_results.items():
            if validation.get('needs_update', False):
                expected_ttl = validation['expected_ttl']
                success = self.set_table_ttl(table_name, expected_ttl)
                fix_results[table_name] = success
                
                if success:
                    logger.info(f"Fixed TTL for {table_name}")
                else:
                    logger.error(f"Failed to fix TTL for {table_name}")
            else:
                fix_results[table_name] = True  # No fix needed
        
        return fix_results
    
    def get_expiring_data_estimate(self, table_name: str, 
                                 hours_ahead: int = 24) -> Dict[str, Any]:
        """Estimate how much data will expire in the next X hours."""
        
        current_ttl = self.get_table_ttl(table_name)
        if not current_ttl:
            return {'error': 'Could not get TTL for table'}
        
        # Calculate expiration window
        now = datetime.now(timezone.utc)
        expire_start = now + timedelta(seconds=current_ttl - hours_ahead * 3600)
        expire_end = now + timedelta(seconds=current_ttl)
        
        # Query for data that will expire
        query = f"""
            SELECT COUNT(*) as expiring_count
            FROM {table_name}
            WHERE created_at >= %s AND created_at < %s
            ALLOW FILTERING
        """
        
        try:
            result = self.session.execute(query, [expire_start, expire_end])
            expiring_count = result.one().expiring_count
            
            return {
                'table_name': table_name,
                'current_ttl_seconds': current_ttl,
                'expiring_in_hours': hours_ahead,
                'expiring_count': expiring_count,
                'expire_window_start': expire_start,
                'expire_window_end': expire_end
            }
            
        except Exception as e:
            logger.error(f"Failed to estimate expiring data for {table_name}: {e}")
            return {'error': str(e)}
    
    def get_table_storage_stats(self, table_name: str) -> Dict[str, Any]:
        """Get storage statistics for a table."""
        
        # Get basic row count
        try:
            count_query = f"SELECT COUNT(*) FROM {table_name}"
            result = self.session.execute(count_query)
            row_count = result.one()[0]
        except:
            row_count = -1
        
        # Get table size from system tables (approximate)
        try:
            size_query = """
                SELECT mean_partition_size, partitions_count
                FROM system.size_estimates
                WHERE keyspace_name = %s AND table_name = %s
            """
            keyspace = self.config['cassandra']['keyspace']
            result = self.session.execute(size_query, [keyspace, table_name])
            
            total_size = 0
            partition_count = 0
            
            for row in result:
                total_size += row.mean_partition_size or 0
                partition_count += 1
            
        except Exception as e:
            logger.warning(f"Could not get size estimates for {table_name}: {e}")
            total_size = -1
            partition_count = -1
        
        return {
            'table_name': table_name,
            'row_count': row_count,
            'estimated_size_bytes': total_size,
            'partition_count': partition_count,
            'current_ttl': self.get_table_ttl(table_name)
        }
    
    def optimize_ttl_based_on_usage(self, table_name: str,
                                   target_storage_gb: float = 1.0) -> Optional[int]:
        """
        Suggest optimal TTL based on storage usage and data patterns.
        
        Args:
            table_name: Name of the table to optimize
            target_storage_gb: Target storage size in GB
            
        Returns:
            Suggested TTL in seconds, or None if optimization not possible
        """
        
        stats = self.get_table_storage_stats(table_name)
        
        if stats['estimated_size_bytes'] <= 0 or stats['row_count'] <= 0:
            logger.warning(f"Cannot optimize TTL for {table_name} - insufficient data")
            return None
        
        current_ttl = stats['current_ttl']
        current_size_gb = stats['estimated_size_bytes'] / (1024**3)
        
        if current_size_gb <= target_storage_gb:
            logger.info(f"Table {table_name} already within target size")
            return current_ttl
        
        # Calculate suggested TTL
        size_ratio = target_storage_gb / current_size_gb
        suggested_ttl = int(current_ttl * size_ratio)
        
        # Ensure minimum TTL of 6 hours for speed layer
        min_ttl = 6 * 3600  # 6 hours
        suggested_ttl = max(suggested_ttl, min_ttl)
        
        logger.info(f"Suggested TTL for {table_name}: {suggested_ttl} seconds "
                   f"(current: {current_ttl}, size: {current_size_gb:.2f}GB)")
        
        return suggested_ttl
    
    def monitor_ttl_health(self) -> Dict[str, Any]:
        """Monitor overall TTL health across all tables."""
        
        health_report = {
            'timestamp': datetime.now(timezone.utc),
            'tables': {},
            'summary': {
                'total_tables': 0,
                'healthy_tables': 0,
                'unhealthy_tables': 0,
                'total_estimated_size_gb': 0.0
            }
        }
        
        for table_name in self.ttl_settings.keys():
            try:
                # Get table stats
                stats = self.get_table_storage_stats(table_name)
                ttl_validation = self.validate_all_ttls().get(table_name, {})
                
                # Calculate health score (0-100)
                health_score = 100
                issues = []
                
                if not ttl_validation.get('is_valid', False):
                    health_score -= 50
                    issues.append('TTL mismatch')
                
                if stats['row_count'] < 0:
                    health_score -= 20
                    issues.append('Cannot determine row count')
                
                if stats['estimated_size_bytes'] < 0:
                    health_score -= 10
                    issues.append('Cannot determine size')
                
                # Size-based health check
                if stats['estimated_size_bytes'] > 0:
                    size_gb = stats['estimated_size_bytes'] / (1024**3)
                    if size_gb > 2.0:  # Warning if table > 2GB
                        health_score -= 15
                        issues.append(f'Large table size: {size_gb:.2f}GB')
                    
                    health_report['summary']['total_estimated_size_gb'] += size_gb
                
                health_report['tables'][table_name] = {
                    'health_score': health_score,
                    'issues': issues,
                    'stats': stats,
                    'ttl_validation': ttl_validation
                }
                
                health_report['summary']['total_tables'] += 1
                if health_score >= 80:
                    health_report['summary']['healthy_tables'] += 1
                else:
                    health_report['summary']['unhealthy_tables'] += 1
                
            except Exception as e:
                logger.error(f"Failed to check health for table {table_name}: {e}")
                health_report['tables'][table_name] = {
                    'health_score': 0,
                    'issues': [f'Health check failed: {e}'],
                    'stats': {},
                    'ttl_validation': {}
                }
                health_report['summary']['unhealthy_tables'] += 1
        
        return health_report
    
    def cleanup_expired_data_manually(self, table_name: str) -> int:
        """
        Manually cleanup expired data (beyond TTL).
        This is usually not needed but can be useful for maintenance.
        """
        
        current_ttl = self.get_table_ttl(table_name)
        if not current_ttl:
            logger.error(f"Cannot cleanup {table_name} - no TTL information")
            return 0
        
        # Calculate cutoff time
        cutoff_time = datetime.now(timezone.utc) - timedelta(seconds=current_ttl)
        
        # Delete old data
        query = f"""
            DELETE FROM {table_name}
            WHERE created_at < %s
        """
        
        try:
            # First count what will be deleted
            count_query = f"""
                SELECT COUNT(*) FROM {table_name}
                WHERE created_at < %s
                ALLOW FILTERING
            """
            result = self.session.execute(count_query, [cutoff_time])
            delete_count = result.one()[0]
            
            if delete_count > 0:
                # Perform deletion
                self.session.execute(query, [cutoff_time])
                logger.info(f"Manually cleaned up {delete_count} expired records from {table_name}")
            else:
                logger.info(f"No expired records found in {table_name}")
            
            return delete_count
            
        except Exception as e:
            logger.error(f"Failed to cleanup expired data from {table_name}: {e}")
            return 0


def main():
    """Test TTL manager functionality."""
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        # Create session (this would normally come from connection manager)
        from cassandra_views.speed_view_manager import CassandraConnectionManager
        
        with CassandraConnectionManager() as conn_mgr:
            session = conn_mgr.get_session()
            ttl_manager = TTLManager(session)
            
            # Validate all TTLs
            validation_results = ttl_manager.validate_all_ttls()
            logger.info(f"TTL Validation Results: {validation_results}")
            
            # Get health report
            health_report = ttl_manager.monitor_ttl_health()
            logger.info(f"TTL Health Report: {health_report['summary']}")
            
            # Fix any TTL issues
            fix_results = ttl_manager.fix_all_ttls()
            logger.info(f"TTL Fix Results: {fix_results}")
            
            logger.info("TTL Manager test completed successfully")
            
    except Exception as e:
        logger.error(f"TTL Manager test failed: {e}")
        raise


if __name__ == "__main__":
    main()
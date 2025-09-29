"""
Main entry point for the movie data ingestion pipeline.
"""
import logging
import signal
import sys
import time
from typing import Optional
import threading
import os

# Add project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from config.config import config
from src.ingestion.tmdb_client import TMDBClient
from src.ingestion.kafka_producer import MovieDataProducer
from src.ingestion.data_extractor import DataExtractor
from src.ingestion.airbyte_manager import AirbyteManager, AirbyteOrchestrator

# Configure logging
log_dir = os.path.join(os.path.dirname(__file__), 'logs')
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, 'ingestion.log')

logging.basicConfig(
    level=getattr(logging, config.log_level),
    format=config.log_format,
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_file)
    ]
)

logger = logging.getLogger(__name__)

class IngestionPipeline:
    """Main ingestion pipeline orchestrator."""
    
    def __init__(self, use_airbyte: bool = True):
        self.use_airbyte = use_airbyte
        self.tmdb_client: Optional[TMDBClient] = None
        self.kafka_producer: Optional[MovieDataProducer] = None
        self.data_extractor: Optional[DataExtractor] = None
        self.airbyte_manager: Optional[AirbyteManager] = None
        self.airbyte_orchestrator: Optional[AirbyteOrchestrator] = None
        self.extraction_thread: Optional[threading.Thread] = None
        self.is_running = False
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        logger.info(f"Received signal {signum}, shutting down...")
        self.shutdown()
    
    def initialize(self):
        """Initialize all components."""
        logger.info("Initializing ingestion pipeline...")
        
        try:
            # Validate configuration
            config.validate()
            
            if self.use_airbyte:
                # Initialize Airbyte components
                logger.info("Initializing Airbyte integration...")
                
                airbyte_host = getattr(config, 'airbyte_host', 'localhost')
                airbyte_port = getattr(config, 'airbyte_port', 8001)
                workspace_id = getattr(config, 'airbyte_workspace_id', None)
                
                if not workspace_id:
                    logger.warning("Airbyte workspace ID not configured. You'll need to set it manually.")
                
                self.airbyte_manager = AirbyteManager(
                    airbyte_host=airbyte_host,
                    airbyte_port=airbyte_port
                )
                
                if workspace_id:
                    self.airbyte_orchestrator = AirbyteOrchestrator(
                        airbyte_manager=self.airbyte_manager,
                        workspace_id=workspace_id
                    )
                
                logger.info("Airbyte integration initialized")
            else:
                # Initialize legacy components
                logger.info("Initializing legacy ingestion components...")
                
                # Initialize TMDB client
                self.tmdb_client = TMDBClient(
                    api_key=config.tmdb_api_key,
                    base_url=config.tmdb_base_url
                )
                logger.info("TMDB client initialized")
                
                # Initialize Kafka producer
                self.kafka_producer = MovieDataProducer(
                    bootstrap_servers=config.kafka_bootstrap_servers,
                    schema_registry_url=config.schema_registry_url
                )
                logger.info("Kafka producer initialized")
                
                # Initialize data extractor
                self.data_extractor = DataExtractor(
                    tmdb_client=self.tmdb_client,
                    kafka_producer=self.kafka_producer
                )
                logger.info("Data extractor initialized")
            
            logger.info("Pipeline initialization completed successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize pipeline: {e}")
            raise
    
    def run_batch_extraction(self):
        """Run a single batch extraction."""
        logger.info("Starting batch extraction...")
        
        try:
            if self.use_airbyte and self.airbyte_orchestrator:
                # Use Airbyte for data extraction
                logger.info("Running batch extraction via Airbyte...")
                
                sync_result = self.airbyte_orchestrator.run_tmdb_sync()
                
                if sync_result['success']:
                    logger.info("Airbyte sync completed successfully")
                    logger.info(f"Sync statistics: {sync_result['stats']}")
                else:
                    logger.error("Airbyte sync failed")
                    for result in sync_result['sync_results']:
                        if result['result']['status'] != 'succeeded':
                            logger.error(f"Failed sync: {result['connection_name']} - {result['result']}")
                
            else:
                # Use legacy data extractor
                if not self.data_extractor:
                    raise RuntimeError("Pipeline not initialized for legacy extraction")
                
                logger.info("Running batch extraction via legacy extractor...")
                
                # Extract popular movies
                movies = self.data_extractor.extract_popular_movies(max_pages=5)
                logger.info(f"Extracted {len(movies)} popular movies")
                
                # Extract trending movies
                trending_movies = self.data_extractor.extract_trending_movies('day')
                logger.info(f"Extracted {len(trending_movies)} trending movies")
                
                # Extract trending people
                trending_people = self.data_extractor.extract_trending_people('day')
                logger.info(f"Extracted {len(trending_people)} trending people")
                
                # Flush Kafka producer
                self.kafka_producer.flush()
                
                # Print statistics
                stats = self.data_extractor.get_stats()
                logger.info(f"Batch extraction completed: {stats}")
            
        except Exception as e:
            logger.error(f"Error during batch extraction: {e}")
            raise
    
    def run_continuous_extraction(self, interval_minutes: int = 60):
        """Run continuous extraction in a separate thread."""
        logger.info("Starting continuous extraction...")
        
        if not self.data_extractor:
            raise RuntimeError("Pipeline not initialized")
        
        self.is_running = True
        
        # Start extraction in a separate thread
        self.extraction_thread = threading.Thread(
            target=self.data_extractor.run_continuous_extraction,
            args=(interval_minutes,),
            daemon=True
        )
        self.extraction_thread.start()
        
        try:
            # Keep main thread alive and monitor
            while self.is_running:
                time.sleep(10)
                
                # Check if extraction thread is still alive
                if not self.extraction_thread.is_alive():
                    logger.warning("Extraction thread has stopped")
                    break
                
                # Log periodic statistics
                stats = self.data_extractor.get_stats()
                logger.info(f"Current stats: {stats}")
                
        except KeyboardInterrupt:
            logger.info("Continuous extraction interrupted")
        finally:
            self.shutdown()
    
    def shutdown(self):
        """Shutdown the pipeline gracefully."""
        logger.info("Shutting down ingestion pipeline...")
        
        self.is_running = False
        
        # Stop data extraction
        if self.data_extractor:
            self.data_extractor.stop_extraction()
        
        # Wait for extraction thread to finish
        if self.extraction_thread and self.extraction_thread.is_alive():
            logger.info("Waiting for extraction thread to finish...")
            self.extraction_thread.join(timeout=30)
        
        # Close Kafka producer
        if self.kafka_producer:
            logger.info("Closing Kafka producer...")
            self.kafka_producer.close()
        
        logger.info("Pipeline shutdown completed")
    
    def get_health_status(self) -> dict:
        """Get health status of the pipeline."""
        status = {
            'pipeline_running': self.is_running,
            'extraction_thread_alive': False,
            'extraction_stats': {}
        }
        
        if self.extraction_thread:
            status['extraction_thread_alive'] = self.extraction_thread.is_alive()
        
        if self.data_extractor:
            status['extraction_stats'] = self.data_extractor.get_stats()
        
        return status

def main():
    """Main entry point."""
    # Check for Airbyte flag
    use_airbyte = '--airbyte' in sys.argv
    use_legacy = '--legacy' in sys.argv
    
    # Remove flags from argv for mode parsing
    cleaned_argv = [arg for arg in sys.argv if arg not in ['--airbyte', '--legacy']]
    
    # Default to Airbyte unless legacy is specified
    if use_legacy:
        use_airbyte = False
    elif not use_airbyte and not use_legacy:
        use_airbyte = True
    
    logger.info(f"Starting ingestion pipeline with {'Airbyte' if use_airbyte else 'legacy'} mode")
    
    pipeline = IngestionPipeline(use_airbyte=use_airbyte)
    
    try:
        # Initialize pipeline
        pipeline.initialize()
        
        # Check command line arguments
        if len(cleaned_argv) > 1:
            mode = cleaned_argv[1].lower()
            
            if mode == 'batch':
                pipeline.run_batch_extraction()
            elif mode == 'continuous':
                interval = int(cleaned_argv[2]) if len(cleaned_argv) > 2 else 60
                pipeline.run_continuous_extraction(interval)
            elif mode == 'status' and use_airbyte:
                # Airbyte-specific status command
                if pipeline.airbyte_orchestrator:
                    stats = pipeline.airbyte_orchestrator.get_sync_statistics()
                    logger.info(f"Airbyte sync statistics: {stats}")
                    
                    active_syncs = pipeline.airbyte_orchestrator.monitor_active_syncs()
                    if active_syncs:
                        logger.info(f"Active syncs: {active_syncs}")
                    else:
                        logger.info("No active syncs")
                else:
                    logger.error("Airbyte orchestrator not initialized")
            else:
                print("Usage: python -m src.ingestion.main [--airbyte|--legacy] [batch|continuous|status] [interval_minutes]")
                sys.exit(1)
        else:
            # Default to continuous mode
            pipeline.run_continuous_extraction()
            
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        sys.exit(1)
    finally:
        pipeline.shutdown()

if __name__ == "__main__":
    main()
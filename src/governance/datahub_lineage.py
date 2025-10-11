"""
DataHub lineage tracking and metadata management utilities.
"""
import logging
import time
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone
import json

logger = logging.getLogger(__name__)

class DataHubLineageTracker:
    """Tracks data lineage and metadata for the movie analytics pipeline."""
    
    def __init__(self, gms_server: str = None, token: str = None):
        """Initialize DataHub lineage tracker."""
        self.gms_server = gms_server or "http://localhost:8080"
        self.token = token
        self.emitter = None
        
        # Import DataHub dependencies only when needed
        self._initialize_datahub_client()
        
        logger.info(f"DataHub lineage tracker initialized with server: {self.gms_server}")
    
    def _initialize_datahub_client(self):
        """Initialize DataHub client with lazy loading."""
        try:
            from datahub.emitter.rest_emitter import DatahubRestEmitter
            from datahub.emitter.mcp import MetadataChangeProposalWrapper
            
            self.emitter = DatahubRestEmitter(
                gms_server=self.gms_server,
                token=self.token
            )
            self.MetadataChangeProposalWrapper = MetadataChangeProposalWrapper
            
        except ImportError as e:
            logger.warning(f"DataHub dependencies not available: {e}")
            self.emitter = None
    
    def emit_dataset_metadata(self, 
                            platform: str,
                            dataset_name: str, 
                            schema_fields: List[Dict[str, Any]] = None,
                            properties: Dict[str, Any] = None,
                            tags: List[str] = None,
                            description: str = None,
                            env: str = "PROD") -> str:
        """Emit dataset metadata to DataHub."""
        if not self.emitter:
            logger.warning("DataHub emitter not available, skipping metadata emission")
            return f"urn:li:dataset:(urn:li:dataPlatform:{platform},movie-pipeline.{dataset_name},{env})"
        
        try:
            from datahub.metadata.schema_classes import DatasetPropertiesClass
            
            # Generate dataset URN
            dataset_urn = f"urn:li:dataset:(urn:li:dataPlatform:{platform},movie-pipeline.{dataset_name},{env})"
            
            # Create dataset properties
            dataset_properties = DatasetPropertiesClass(
                description=description or f"Dataset {dataset_name} from {platform}",
                customProperties=properties or {},
                created=int(time.time() * 1000),
                lastModified=int(time.time() * 1000)
            )
            
            # Emit dataset properties
            mcp = self.MetadataChangeProposalWrapper(
                entityType="dataset",
                entityUrn=dataset_urn,
                aspectName="datasetProperties",
                aspect=dataset_properties
            )
            self.emitter.emit_mcp(mcp)
            
            # Emit tags if provided
            if tags:
                global_tags = self._create_global_tags(tags)
                if global_tags:
                    mcp = self.MetadataChangeProposalWrapper(
                        entityType="dataset",
                        entityUrn=dataset_urn,
                        aspectName="globalTags",
                        aspect=global_tags
                    )
                    self.emitter.emit_mcp(mcp)
            
            logger.info(f"Emitted metadata for dataset: {dataset_urn}")
            return dataset_urn
            
        except Exception as e:
            logger.error(f"Error emitting dataset metadata for {dataset_name}: {e}")
            return f"urn:li:dataset:(urn:li:dataPlatform:{platform},movie-pipeline.{dataset_name},{env})"
    
    def emit_job_metadata(self,
                         platform: str,
                         job_name: str,
                         job_type: str,
                         inputs: List[str],
                         outputs: List[str],
                         properties: Dict[str, Any] = None,
                         description: str = None,
                         env: str = "PROD") -> str:
        """Emit data job metadata to DataHub."""
        if not self.emitter:
            logger.warning("DataHub emitter not available, skipping job metadata emission")
            return f"urn:li:dataJob:(urn:li:dataFlow:({platform},movie-pipeline,{env}),{job_name})"
        
        try:
            from datahub.metadata.schema_classes import DataJobInfoClass, DataJobInputOutputClass
            
            # Generate job URN
            job_urn = f"urn:li:dataJob:(urn:li:dataFlow:({platform},movie-pipeline,{env}),{job_name})"
            
            # Create job info
            job_info = DataJobInfoClass(
                name=job_name,
                type=job_type,
                description=description or f"{job_type} job: {job_name}",
                customProperties=properties or {}
            )
            
            # Emit job info
            mcp = self.MetadataChangeProposalWrapper(
                entityType="dataJob",
                entityUrn=job_urn,
                aspectName="dataJobInfo",
                aspect=job_info
            )
            self.emitter.emit_mcp(mcp)
            
            # Create and emit job input/output lineage
            job_io = DataJobInputOutputClass(
                inputDatasets=inputs,
                outputDatasets=outputs,
                inputDatajobs=[]
            )
            
            mcp = self.MetadataChangeProposalWrapper(
                entityType="dataJob",
                entityUrn=job_urn,
                aspectName="dataJobInputOutput",
                aspect=job_io
            )
            self.emitter.emit_mcp(mcp)
            
            logger.info(f"Emitted metadata for job: {job_urn}")
            return job_urn
            
        except Exception as e:
            logger.error(f"Error emitting job metadata for {job_name}: {e}")
            return f"urn:li:dataJob:(urn:li:dataFlow:({platform},movie-pipeline,{env}),{job_name})"
    
    def emit_lineage(self,
                    downstream_urn: str,
                    upstream_urns: List[str],
                    lineage_type: str = "TRANSFORMED") -> None:
        """Emit lineage information between datasets."""
        if not self.emitter:
            logger.warning("DataHub emitter not available, skipping lineage emission")
            return
        
        try:
            from datahub.metadata.schema_classes import (
                UpstreamLineageClass, 
                UpstreamClass,
                DatasetLineageTypeClass
            )
            
            # Create upstream lineage
            upstreams = [
                UpstreamClass(
                    dataset=upstream_urn,
                    type=DatasetLineageTypeClass.TRANSFORMED
                )
                for upstream_urn in upstream_urns
            ]
            
            upstream_lineage = UpstreamLineageClass(upstreams=upstreams)
            
            # Emit lineage
            mcp = self.MetadataChangeProposalWrapper(
                entityType="dataset",
                entityUrn=downstream_urn,
                aspectName="upstreamLineage",
                aspect=upstream_lineage
            )
            self.emitter.emit_mcp(mcp)
            
            logger.info(f"Emitted lineage: {upstream_urns} -> {downstream_urn}")
            
        except Exception as e:
            logger.error(f"Error emitting lineage: {e}")
    
    def track_kafka_ingestion(self,
                            topic: str,
                            source_system: str,
                            properties: Dict[str, Any] = None) -> Dict[str, str]:
        """Track Kafka topic ingestion from external source."""
        try:
            # Create source dataset URN
            source_urn = f"urn:li:dataset:(urn:li:dataPlatform:{source_system},movie-pipeline.api,PROD)"
            
            # Create Kafka topic URN  
            topic_urn = f"urn:li:dataset:(urn:li:dataPlatform:kafka,movie-pipeline.{topic},PROD)"
            
            # Emit source metadata
            self.emit_dataset_metadata(
                platform=source_system,
                dataset_name='api',
                description=f"{source_system.upper()} API source",
                tags=['external-api', 'source'],
                properties=properties or {}
            )
            
            # Emit Kafka topic metadata
            topic_props = {
                'topic': topic,
                'bootstrap_servers': 'localhost:9092'
            }
            if properties:
                topic_props.update(properties)
            
            self.emit_dataset_metadata(
                platform='kafka',
                dataset_name=topic,
                description=f"Kafka topic for {topic} data",
                tags=['kafka', 'streaming', 'real-time'],
                properties=topic_props
            )
            
            # Emit ingestion job
            job_urn = self.emit_job_metadata(
                platform='kafka',
                job_name=f'ingest_{topic}',
                job_type='INGESTION',
                inputs=[source_urn],
                outputs=[topic_urn],
                description=f"Ingestion job for {topic} from {source_system}"
            )
            
            # Emit lineage
            self.emit_lineage(topic_urn, [source_urn])
            
            return {
                'source_urn': source_urn,
                'topic_urn': topic_urn,
                'job_urn': job_urn
            }
            
        except Exception as e:
            logger.error(f"Error tracking Kafka ingestion for {topic}: {e}")
            return {
                'source_urn': f"urn:li:dataset:(urn:li:dataPlatform:{source_system},movie-pipeline.api,PROD)",
                'topic_urn': f"urn:li:dataset:(urn:li:dataPlatform:kafka,movie-pipeline.{topic},PROD)",
                'job_urn': f"urn:li:dataJob:(urn:li:dataFlow:(kafka,movie-pipeline,PROD),ingest_{topic})"
            }
    
    def track_spark_processing(self,
                             job_name: str,
                             input_topics: List[str],
                             output_layers: List[str],
                             properties: Dict[str, Any] = None) -> Dict[str, str]:
        """Track Spark processing job lineage."""
        try:
            # Create input URNs (Kafka topics)
            input_urns = [
                f"urn:li:dataset:(urn:li:dataPlatform:kafka,movie-pipeline.{topic},PROD)"
                for topic in input_topics
            ]
            
            # Create output URNs (Storage layers)
            output_urns = [
                f"urn:li:dataset:(urn:li:dataPlatform:s3,movie-pipeline.{layer},PROD)"
                for layer in output_layers
            ]
            
            # Emit output dataset metadata
            for layer in output_layers:
                layer_props = {
                    'layer': layer.split('/')[0] if '/' in layer else 'bronze',
                    'format': 'parquet'
                }
                if properties:
                    layer_props.update(properties)
                
                self.emit_dataset_metadata(
                    platform='s3',
                    dataset_name=layer,
                    description=f"Storage layer: {layer}",
                    tags=['storage', 'processed', layer.split('/')[0] if '/' in layer else 'bronze'],
                    properties=layer_props
                )
            
            # Emit Spark job metadata
            job_urn = self.emit_job_metadata(
                platform='spark',
                job_name=job_name,
                job_type='PROCESSING',
                inputs=input_urns,
                outputs=output_urns,
                description=f"Spark processing job: {job_name}",
                properties=properties or {}
            )
            
            # Emit lineage for each output
            for output_urn in output_urns:
                self.emit_lineage(output_urn, input_urns)
            
            return {
                'job_urn': job_urn,
                'input_urns': input_urns,
                'output_urns': output_urns
            }
            
        except Exception as e:
            logger.error(f"Error tracking Spark processing for {job_name}: {e}")
            return {
                'job_urn': f"urn:li:dataJob:(urn:li:dataFlow:(spark,movie-pipeline,PROD),{job_name})",
                'input_urns': [f"urn:li:dataset:(urn:li:dataPlatform:kafka,movie-pipeline.{topic},PROD)" for topic in input_topics],
                'output_urns': [f"urn:li:dataset:(urn:li:dataPlatform:s3,movie-pipeline.{layer},PROD)" for layer in output_layers]
            }
    
    def track_mongodb_serving(self,
                            collection: str,
                            source_layers: List[str],
                            properties: Dict[str, Any] = None) -> Dict[str, str]:
        """Track MongoDB serving layer lineage."""
        try:
            # Create source URNs (Storage layers)
            source_urns = [
                f"urn:li:dataset:(urn:li:dataPlatform:s3,movie-pipeline.{layer},PROD)"
                for layer in source_layers
            ]
            
            # Create MongoDB collection URN
            collection_urn = f"urn:li:dataset:(urn:li:dataPlatform:mongodb,movie-pipeline.moviedb.{collection},PROD)"
            
            # Emit MongoDB collection metadata
            mongo_props = {
                'database': 'moviedb',
                'collection': collection
            }
            if properties:
                mongo_props.update(properties)
            
            self.emit_dataset_metadata(
                platform='mongodb',
                dataset_name=f"moviedb.{collection}",
                description=f"MongoDB collection: {collection}",
                tags=['mongodb', 'serving', 'nosql'],
                properties=mongo_props
            )
            
            # Emit serving job metadata
            job_urn = self.emit_job_metadata(
                platform='mongodb',
                job_name=f'serve_{collection}',
                job_type='SERVING',
                inputs=source_urns,
                outputs=[collection_urn],
                description=f"Serving job for {collection} collection"
            )
            
            # Emit lineage
            self.emit_lineage(collection_urn, source_urns)
            
            return {
                'collection_urn': collection_urn,
                'job_urn': job_urn,
                'source_urns': source_urns
            }
            
        except Exception as e:
            logger.error(f"Error tracking MongoDB serving for {collection}: {e}")
            return {
                'collection_urn': f"urn:li:dataset:(urn:li:dataPlatform:mongodb,movie-pipeline.moviedb.{collection},PROD)",
                'job_urn': f"urn:li:dataJob:(urn:li:dataFlow:(mongodb,movie-pipeline,PROD),serve_{collection})",
                'source_urns': [f"urn:li:dataset:(urn:li:dataPlatform:s3,movie-pipeline.{layer},PROD)" for layer in source_layers]
            }
    
    def _create_global_tags(self, tags: List[str]):
        """Create global tags for a dataset."""
        try:
            from datahub.metadata.schema_classes import GlobalTagsClass, TagAssociationClass
            
            tag_associations = [
                TagAssociationClass(tag=f"urn:li:tag:{tag}")
                for tag in tags
            ]
            
            return GlobalTagsClass(tags=tag_associations)
        except ImportError:
            logger.warning("DataHub schema classes not available for tags")
            return None
    
    def close(self):
        """Close the DataHub emitter."""
        if self.emitter:
            self.emitter.close()

# Global lineage tracker instance - will be initialized when needed
lineage_tracker = None

def get_lineage_tracker() -> DataHubLineageTracker:
    """Get or create the global lineage tracker instance."""
    global lineage_tracker
    if lineage_tracker is None:
        lineage_tracker = DataHubLineageTracker()
    return lineage_tracker
"""
Airbyte integration module for movie data pipeline.
Provides utilities to interact with Airbyte API and monitor syncs.
"""
import logging
import time
import requests
from typing import Dict, List, Optional, Any
from datetime import datetime

logger = logging.getLogger(__name__)

class AirbyteManager:
    """Manager for Airbyte operations and sync monitoring."""
    
    def __init__(self, airbyte_host: str = "localhost", airbyte_port: int = 8001):
        self.base_url = f"http://{airbyte_host}:{airbyte_port}/api/v1"
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })
    
    def _make_request(self, method: str, endpoint: str, data: Optional[Dict] = None) -> Optional[Dict]:
        """Make a request to Airbyte API."""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        
        try:
            if method.upper() == 'GET':
                response = self.session.get(url, params=data)
            elif method.upper() == 'POST':
                response = self.session.post(url, json=data)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
            
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Error making {method} request to {url}: {e}")
            return None
    
    def get_workspaces(self) -> List[Dict[str, Any]]:
        """Get all workspaces."""
        result = self._make_request('POST', 'workspaces/list')
        return result.get('workspaces', []) if result else []
    
    def get_sources(self, workspace_id: str) -> List[Dict[str, Any]]:
        """Get all sources in a workspace."""
        result = self._make_request('POST', 'sources/list', {'workspaceId': workspace_id})
        return result.get('sources', []) if result else []
    
    def get_destinations(self, workspace_id: str) -> List[Dict[str, Any]]:
        """Get all destinations in a workspace."""
        result = self._make_request('POST', 'destinations/list', {'workspaceId': workspace_id})
        return result.get('destinations', []) if result else []
    
    def get_connections(self, workspace_id: str) -> List[Dict[str, Any]]:
        """Get all connections in a workspace."""
        result = self._make_request('POST', 'connections/list', {'workspaceId': workspace_id})
        return result.get('connections', []) if result else []
    
    def trigger_sync(self, connection_id: str) -> Optional[Dict[str, Any]]:
        """Trigger a manual sync for a connection."""
        logger.info(f"Triggering sync for connection: {connection_id}")
        return self._make_request('POST', 'connections/sync', {'connectionId': connection_id})
    
    def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get status of a specific job."""
        return self._make_request('POST', 'jobs/get', {'id': job_id})
    
    def wait_for_sync_completion(self, job_id: str, timeout: int = 3600, poll_interval: int = 30) -> Dict[str, Any]:
        """Wait for a sync job to complete."""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            job_status = self.get_job_status(job_id)
            
            if not job_status:
                logger.error(f"Failed to get status for job {job_id}")
                return {'status': 'error', 'message': 'Failed to get job status'}
            
            status = job_status.get('job', {}).get('status')
            logger.info(f"Job {job_id} status: {status}")
            
            if status in ['succeeded', 'failed', 'cancelled']:
                return {
                    'status': status,
                    'job': job_status.get('job', {}),
                    'duration': time.time() - start_time
                }
            
            time.sleep(poll_interval)
        
        return {'status': 'timeout', 'message': f'Job did not complete within {timeout} seconds'}
    
    def get_connection_status(self, connection_id: str) -> Optional[Dict[str, Any]]:
        """Get the current status of a connection."""
        return self._make_request('POST', 'connections/get', {'connectionId': connection_id})
    
    def list_recent_jobs(self, connection_id: str, limit: int = 10) -> List[Dict[str, Any]]:
        """List recent jobs for a connection."""
        result = self._make_request('POST', 'jobs/list', {
            'configTypes': ['sync'],
            'configId': connection_id,
            'pagination': {'pageSize': limit}
        })
        return result.get('jobs', []) if result else []


class AirbyteOrchestrator:
    """Orchestrates Airbyte syncs for the movie data pipeline."""
    
    def __init__(self, airbyte_manager: AirbyteManager, workspace_id: str):
        self.airbyte_manager = airbyte_manager
        self.workspace_id = workspace_id
        self.sync_stats = {
            'syncs_triggered': 0,
            'syncs_completed': 0,
            'syncs_failed': 0,
            'total_records': 0,
            'start_time': None,
            'end_time': None
        }
    
    def run_tmdb_sync(self, connection_names: List[str] = None) -> Dict[str, Any]:
        """Run TMDB data sync through Airbyte."""
        logger.info("Starting TMDB data sync via Airbyte")
        self.sync_stats['start_time'] = datetime.now()
        
        # Get all connections if none specified
        if connection_names is None:
            connection_names = ['TMDB to Kafka Connection', 'TMDB to MongoDB Connection']
        
        connections = self.airbyte_manager.get_connections(self.workspace_id)
        
        sync_results = []
        
        for connection in connections:
            connection_name = connection.get('name', '')
            
            if connection_names and connection_name not in connection_names:
                continue
            
            connection_id = connection.get('connectionId')
            if not connection_id:
                logger.warning(f"No connection ID found for {connection_name}")
                continue
            
            logger.info(f"Triggering sync for connection: {connection_name}")
            
            # Trigger sync
            sync_result = self.airbyte_manager.trigger_sync(connection_id)
            if not sync_result:
                logger.error(f"Failed to trigger sync for {connection_name}")
                self.sync_stats['syncs_failed'] += 1
                continue
            
            job_id = sync_result.get('job', {}).get('id')
            if not job_id:
                logger.error(f"No job ID returned for {connection_name}")
                self.sync_stats['syncs_failed'] += 1
                continue
            
            self.sync_stats['syncs_triggered'] += 1
            
            # Wait for completion
            completion_result = self.airbyte_manager.wait_for_sync_completion(job_id)
            
            if completion_result['status'] == 'succeeded':
                self.sync_stats['syncs_completed'] += 1
                logger.info(f"Sync completed successfully for {connection_name}")
            else:
                self.sync_stats['syncs_failed'] += 1
                logger.error(f"Sync failed for {connection_name}: {completion_result}")
            
            sync_results.append({
                'connection_name': connection_name,
                'connection_id': connection_id,
                'job_id': job_id,
                'result': completion_result
            })
        
        self.sync_stats['end_time'] = datetime.now()
        
        return {
            'sync_results': sync_results,
            'stats': self.sync_stats,
            'success': self.sync_stats['syncs_completed'] > 0
        }
    
    def monitor_active_syncs(self) -> List[Dict[str, Any]]:
        """Monitor all active syncs in the workspace."""
        connections = self.airbyte_manager.get_connections(self.workspace_id)
        active_syncs = []
        
        for connection in connections:
            connection_id = connection.get('connectionId')
            recent_jobs = self.airbyte_manager.list_recent_jobs(connection_id, limit=5)
            
            for job in recent_jobs:
                if job.get('status') in ['pending', 'running']:
                    active_syncs.append({
                        'connection_name': connection.get('name'),
                        'job_id': job.get('id'),
                        'status': job.get('status'),
                        'created_at': job.get('createdAt')
                    })
        
        return active_syncs
    
    def get_sync_statistics(self) -> Dict[str, Any]:
        """Get comprehensive sync statistics."""
        connections = self.airbyte_manager.get_connections(self.workspace_id)
        stats = {
            'total_connections': len(connections),
            'connections': []
        }
        
        for connection in connections:
            connection_id = connection.get('connectionId')
            recent_jobs = self.airbyte_manager.list_recent_jobs(connection_id, limit=10)
            
            connection_stats = {
                'name': connection.get('name'),
                'id': connection_id,
                'status': connection.get('status'),
                'total_jobs': len(recent_jobs),
                'successful_jobs': sum(1 for job in recent_jobs if job.get('status') == 'succeeded'),
                'failed_jobs': sum(1 for job in recent_jobs if job.get('status') == 'failed'),
                'last_sync': recent_jobs[0].get('createdAt') if recent_jobs else None
            }
            
            stats['connections'].append(connection_stats)
        
        return stats
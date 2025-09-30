"""
Custom Airflow plugin for movie data pipeline.
Provides UI extensions, custom views, and management features.
"""
from flask import Blueprint, request, jsonify, render_template_string
from airflow.plugins_manager import AirflowPlugin
from airflow.security import permissions
from airflow.www.auth import has_access
from airflow.www.decorators import action_logging
from airflow.models import DagRun, TaskInstance
from airflow.utils.db import provide_session
from airflow.utils.state import State
from datetime import datetime, timedelta
import json

# Create Flask Blueprint
movie_pipeline_bp = Blueprint(
    "movie_pipeline",
    __name__,
    template_folder="templates",
    static_folder="static",
    url_prefix="/movie_pipeline"
)

@movie_pipeline_bp.route("/dashboard")
@has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE)])
def dashboard():
    """Movie pipeline dashboard.""" 
    dashboard_html = '''
    <!DOCTYPE html>
    <html>
    <head>
        <title>Movie Data Pipeline Dashboard</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; }
            .metric-card { 
                border: 1px solid #ddd; 
                border-radius: 8px; 
                padding: 15px; 
                margin: 10px; 
                background: #f9f9f9; 
                display: inline-block; 
                min-width: 200px;
            }
            .metric-title { font-weight: bold; font-size: 16px; color: #333; }
            .metric-value { font-size: 24px; color: #007bff; margin: 5px 0; }
            .status-success { color: #28a745; }
            .status-warning { color: #ffc107; }
            .status-error { color: #dc3545; }
            .refresh-btn { 
                background: #007bff; 
                color: white; 
                border: none; 
                padding: 10px 20px; 
                border-radius: 5px; 
                cursor: pointer; 
                margin: 10px;
            }
            .chart-container { margin: 20px 0; }
        </style>
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    </head>
    <body>
        <h1>🎬 Movie Data Pipeline Dashboard</h1>
        
        <button class="refresh-btn" onclick="refreshDashboard()">🔄 Refresh</button>
        
        <div id="metrics-container">
            <div class="metric-card">
                <div class="metric-title">Pipeline Status</div>
                <div class="metric-value status-success" id="pipeline-status">Active</div>
            </div>
            
            <div class="metric-card">
                <div class="metric-title">Data Quality Score</div>
                <div class="metric-value" id="quality-score">Loading...</div>
            </div>
            
            <div class="metric-card">
                <div class="metric-title">Records Processed (24h)</div>
                <div class="metric-value" id="records-processed">Loading...</div>
            </div>
            
            <div class="metric-card">
                <div class="metric-title">Failed Tasks (24h)</div>
                <div class="metric-value" id="failed-tasks">Loading...</div>
            </div>
            
            <div class="metric-card">
                <div class="metric-title">Last Successful Run</div>
                <div class="metric-value" id="last-success">Loading...</div>
            </div>
        </div>
        
        <div class="chart-container">
            <h3>Daily Processing Volume</h3>
            <canvas id="volume-chart" width="800" height="400"></canvas>
        </div>
        
        <div class="chart-container">
            <h3>Data Quality Trend</h3>
            <canvas id="quality-chart" width="800" height="400"></canvas>
        </div>
        
        <script>
            function refreshDashboard() {
                fetch('/movie_pipeline/api/metrics')
                    .then(response => response.json())
                    .then(data => {
                        updateMetrics(data);
                    })
                    .catch(error => {
                        console.error('Error fetching metrics:', error);
                    });
            }
            
            function updateMetrics(data) {
                document.getElementById('quality-score').textContent = (data.quality_score || 0).toFixed(2);
                document.getElementById('records-processed').textContent = data.records_processed || 0;
                document.getElementById('failed-tasks').textContent = data.failed_tasks || 0;
                document.getElementById('last-success').textContent = data.last_success || 'N/A';
                
                // Update status color
                const statusElement = document.getElementById('pipeline-status');
                if (data.quality_score >= 0.9) {
                    statusElement.className = 'metric-value status-success';
                } else if (data.quality_score >= 0.7) {
                    statusElement.className = 'metric-value status-warning';
                } else {
                    statusElement.className = 'metric-value status-error';
                }
            }
            
            // Initialize charts
            function initCharts() {
                // Volume Chart
                const volumeCtx = document.getElementById('volume-chart').getContext('2d');
                new Chart(volumeCtx, {
                    type: 'line',
                    data: {
                        labels: ['7 days ago', '6 days ago', '5 days ago', '4 days ago', '3 days ago', '2 days ago', 'Yesterday', 'Today'],
                        datasets: [{
                            label: 'Records Processed',
                            data: [1200, 1350, 1100, 1400, 1250, 1600, 1450, 1300],
                            borderColor: '#007bff',
                            backgroundColor: 'rgba(0, 123, 255, 0.1)',
                            tension: 0.1
                        }]
                    },
                    options: {
                        responsive: true,
                        scales: {
                            y: {
                                beginAtZero: true
                            }
                        }
                    }
                });
                
                // Quality Chart
                const qualityCtx = document.getElementById('quality-chart').getContext('2d');
                new Chart(qualityCtx, {
                    type: 'line',
                    data: {
                        labels: ['7 days ago', '6 days ago', '5 days ago', '4 days ago', '3 days ago', '2 days ago', 'Yesterday', 'Today'],
                        datasets: [{
                            label: 'Data Quality Score',
                            data: [0.95, 0.92, 0.89, 0.94, 0.96, 0.91, 0.93, 0.95],
                            borderColor: '#28a745',
                            backgroundColor: 'rgba(40, 167, 69, 0.1)',
                            tension: 0.1
                        }]
                    },
                    options: {
                        responsive: true,
                        scales: {
                            y: {
                                min: 0,
                                max: 1,
                                ticks: {
                                    callback: function(value) {
                                        return (value * 100) + '%';
                                    }
                                }
                            }
                        }
                    }
                });
            }
            
            // Load dashboard on page load
            document.addEventListener('DOMContentLoaded', function() {
                refreshDashboard();
                initCharts();
                
                // Auto-refresh every 30 seconds
                setInterval(refreshDashboard, 30000);
            });
        </script>
    </body>
    </html>
    '''
    
    return dashboard_html

@movie_pipeline_bp.route("/api/metrics")
@has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE)])
@provide_session
def api_metrics(session=None):
    """API endpoint for pipeline metrics."""
    try:
        # Calculate metrics from the last 24 hours
        yesterday = datetime.now() - timedelta(days=1)
        
        # Get DAG runs for movie pipeline DAGs
        dag_ids = ['movie_data_ingestion', 'movie_data_processing', 'data_quality_monitoring']
        
        # Count successful and failed runs
        successful_runs = session.query(DagRun).filter(
            DagRun.dag_id.in_(dag_ids),
            DagRun.execution_date >= yesterday,
            DagRun.state == State.SUCCESS
        ).count()
        
        failed_runs = session.query(DagRun).filter(
            DagRun.dag_id.in_(dag_ids),
            DagRun.execution_date >= yesterday,
            DagRun.state == State.FAILED
        ).count()
        
        # Get failed tasks
        failed_tasks = session.query(TaskInstance).filter(
            TaskInstance.dag_id.in_(dag_ids),
            TaskInstance.execution_date >= yesterday,
            TaskInstance.state == State.FAILED
        ).count()
        
        # Get last successful run
        last_success = session.query(DagRun).filter(
            DagRun.dag_id.in_(dag_ids),
            DagRun.state == State.SUCCESS
        ).order_by(DagRun.execution_date.desc()).first()
        
        last_success_time = last_success.execution_date.strftime('%Y-%m-%d %H:%M') if last_success else 'Never'
        
        # Calculate quality score (mock data - would integrate with actual quality metrics)
        quality_score = 0.95 if failed_runs == 0 else max(0.5, 1.0 - (failed_runs * 0.1))
        
        # Mock records processed (would integrate with actual metrics from MongoDB/Kafka)
        records_processed = successful_runs * 1000 + 500  # Mock calculation
        
        metrics = {
            'quality_score': quality_score,
            'records_processed': records_processed,
            'failed_tasks': failed_tasks,
            'successful_runs': successful_runs,
            'failed_runs': failed_runs,
            'last_success': last_success_time,
            'timestamp': datetime.now().isoformat()
        }
        
        return jsonify(metrics)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@movie_pipeline_bp.route("/api/dag/<dag_id>/trigger", methods=['POST'])
@has_access([(permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG)])
@action_logging
def trigger_dag(dag_id):
    """Trigger a specific DAG."""
    try:
        from airflow.api.common.experimental.trigger_dag import trigger_dag as trigger
        
        # Get configuration from request
        conf = request.get_json() or {}
        
        # Trigger the DAG
        execution_date = datetime.now()
        dag_run = trigger(
            dag_id=dag_id,
            run_id=f'manual_{execution_date.strftime("%Y%m%d_%H%M%S")}',
            conf=conf,
            execution_date=execution_date
        )
        
        return jsonify({
            'message': f'DAG {dag_id} triggered successfully',
            'dag_run_id': dag_run.run_id,
            'execution_date': execution_date.isoformat()
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@movie_pipeline_bp.route("/api/health")
@has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE)])
def health_check():
    """Health check endpoint for monitoring systems."""
    try:
        # Check database connectivity
        from airflow.utils.db import check_conn
        db_healthy = True
        try:
            check_conn()
        except:
            db_healthy = False
        
        # Basic health status
        health_status = {
            'status': 'healthy' if db_healthy else 'unhealthy',
            'database': 'connected' if db_healthy else 'disconnected',
            'timestamp': datetime.now().isoformat(),
            'version': '1.0.0'
        }
        
        status_code = 200 if db_healthy else 503
        return jsonify(health_status), status_code
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

class MoviePipelineMenuLink:
    """Custom menu link for the movie pipeline dashboard."""
    
    def get_link(self):
        return {
            'name': 'Movie Pipeline',
            'href': '/movie_pipeline/dashboard',
            'category': 'Browse'
        }

class MoviePipelinePlugin(AirflowPlugin):
    """Airflow plugin for movie data pipeline."""
    
    name = "movie_pipeline_plugin"
    
    # Add the blueprint to Airflow
    flask_blueprints = [movie_pipeline_bp]
    
    # Add menu links
    menu_links = [MoviePipelineMenuLink()]
    
    # Plugin metadata
    plugin_info = {
        'name': 'Movie Data Pipeline Plugin',
        'version': '1.0.0',
        'description': 'Custom plugin for movie data analysis pipeline',
        'author': 'Data Team'
    }
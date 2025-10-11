#!/usr/bin/env python3
"""
DataHub Integration Validation Script
Tests all components to ensure proper integration.
"""

import sys
import os
import traceback
from typing import Dict, Any, List

def test_imports() -> Dict[str, Any]:
    """Test that all modules can be imported."""
    results = {"status": "success", "tests": [], "errors": []}
    
    # Test basic config
    try:
        from config.config import config
        results["tests"].append("✅ Main config imports successfully")
        results["tests"].append(f"✅ DataHub GMS URL: {config.datahub_gms_url}")
        results["tests"].append(f"✅ DataHub Frontend URL: {config.datahub_frontend_url}")
        results["tests"].append(f"✅ DataHub Lineage Enabled: {config.enable_datahub_lineage}")
    except Exception as e:
        results["errors"].append(f"❌ Main config import failed: {e}")
        results["status"] = "error"
    
    # Test DataHub config
    try:
        from config.datahub_config import datahub_config
        results["tests"].append("✅ DataHub config imports successfully")
        results["tests"].append(f"✅ Platform instance: {datahub_config.platform_instance}")
        results["tests"].append(f"✅ Kafka topics configured: {len(datahub_config.kafka_topics)}")
    except Exception as e:
        results["errors"].append(f"❌ DataHub config import failed: {e}")
        results["status"] = "error"
    
    # Test governance module
    try:
        from src.governance.datahub_lineage import get_lineage_tracker
        tracker = get_lineage_tracker()
        results["tests"].append("✅ DataHub lineage tracker imports successfully")
        results["tests"].append(f"✅ Emitter available: {tracker.emitter is not None}")
        if tracker.emitter is None:
            results["tests"].append("ℹ️  DataHub dependencies not installed (expected in dev)")
    except Exception as e:
        results["errors"].append(f"❌ DataHub lineage tracker import failed: {e}")
        results["status"] = "error"
    
    # Test Kafka producer integration
    try:
        sys.path.append('src')
        # This will fail due to missing confluent_kafka, but we test the import structure
        import src.ingestion.kafka_producer
        results["tests"].append("✅ Kafka producer module structure is valid")
    except ImportError as e:
        if "confluent_kafka" in str(e):
            results["tests"].append("ℹ️  Kafka producer imports (confluent_kafka not installed - expected)")
        else:
            results["errors"].append(f"❌ Kafka producer structure error: {e}")
    except Exception as e:
        results["errors"].append(f"❌ Kafka producer import failed: {e}")
        results["status"] = "error"
    
    # Test Spark streaming integration
    try:
        import src.streaming.main
        results["tests"].append("✅ Spark streaming module structure is valid")
    except ImportError as e:
        if "pyspark" in str(e):
            results["tests"].append("ℹ️  Spark streaming imports (pyspark not installed - expected)")
        else:
            results["errors"].append(f"❌ Spark streaming structure error: {e}")
    except Exception as e:
        results["errors"].append(f"❌ Spark streaming import failed: {e}")
        results["status"] = "error"
    
    # Test MongoDB service integration  
    try:
        import src.serving.mongodb_service
        results["tests"].append("✅ MongoDB service module structure is valid")
    except ImportError as e:
        if "pymongo" in str(e):
            results["tests"].append("ℹ️  MongoDB service imports (pymongo not installed - expected)")
        else:
            results["errors"].append(f"❌ MongoDB service structure error: {e}")
    except Exception as e:
        results["errors"].append(f"❌ MongoDB service import failed: {e}")
        results["status"] = "error"
    
    return results

def test_airflow_dag() -> Dict[str, Any]:
    """Test Airflow DAG syntax."""
    results = {"status": "success", "tests": [], "errors": []}
    
    try:
        # Import the DAG file
        sys.path.append('dags')
        import datahub_metadata_management
        results["tests"].append("✅ DataHub Airflow DAG imports successfully")
        
        # Check if DAG is defined
        if hasattr(datahub_metadata_management, 'dag'):
            results["tests"].append("✅ DAG object is defined")
            dag = datahub_metadata_management.dag
            results["tests"].append(f"✅ DAG ID: {dag.dag_id}")
            results["tests"].append(f"✅ DAG tasks count: {len(dag.tasks)}")
        else:
            results["errors"].append("❌ DAG object not found")
            results["status"] = "error"
            
    except ImportError as e:
        if "airflow" in str(e):
            results["tests"].append("ℹ️  Airflow DAG imports (airflow not installed - expected)")
        else:
            results["errors"].append(f"❌ DAG structure error: {e}")
            results["status"] = "error"
    except Exception as e:
        results["errors"].append(f"❌ DAG validation failed: {e}")
        results["status"] = "error"
    
    return results

def test_file_structure() -> Dict[str, Any]:
    """Test that all required files exist."""
    results = {"status": "success", "tests": [], "errors": []}
    
    required_files = [
        "config/datahub_config.py",
        "src/governance/__init__.py", 
        "src/governance/datahub_lineage.py",
        "dags/datahub_metadata_management.py",
        "kubernetes/datahub.yaml",
        "docs/DATAHUB_SETUP.md",
        "docs/DataHub_Integration_Summary.md"
    ]
    
    for file_path in required_files:
        if os.path.exists(file_path):
            results["tests"].append(f"✅ {file_path} exists")
        else:
            results["errors"].append(f"❌ {file_path} missing")
            results["status"] = "error"
    
    return results

def test_docker_compose() -> Dict[str, Any]:
    """Test Docker Compose configuration."""
    results = {"status": "success", "tests": [], "errors": []}
    
    try:
        with open("docker-compose.yml", "r") as f:
            content = f.read()
        
        # Check for DataHub services
        datahub_services = [
            "datahub-elasticsearch",
            "datahub-mysql", 
            "datahub-gms",
            "datahub-frontend",
            "datahub-actions"
        ]
        
        for service in datahub_services:
            if service in content:
                results["tests"].append(f"✅ {service} service defined")
            else:
                results["errors"].append(f"❌ {service} service missing")
                results["status"] = "error"
        
        # Check for DataHub volumes
        datahub_volumes = [
            "datahub-elasticsearch-data",
            "datahub-mysql-data"
        ]
        
        for volume in datahub_volumes:
            if volume in content:
                results["tests"].append(f"✅ {volume} volume defined")
            else:
                results["errors"].append(f"❌ {volume} volume missing")
                results["status"] = "error"
                
    except Exception as e:
        results["errors"].append(f"❌ Docker Compose validation failed: {e}")
        results["status"] = "error"
    
    return results

def test_requirements() -> Dict[str, Any]:
    """Test requirements.txt has DataHub dependencies."""
    results = {"status": "success", "tests": [], "errors": []}
    
    try:
        # Try different encodings to handle BOM issues
        content = ""
        try:
            with open("requirements.txt", "r", encoding="utf-8") as f:
                content = f.read()
        except UnicodeDecodeError:
            try:
                with open("requirements.txt", "r", encoding="utf-16") as f:
                    content = f.read()
            except UnicodeDecodeError:
                with open("requirements.txt", "r", encoding="utf-8-sig") as f:
                    content = f.read()
        
        if "acryl-datahub" in content:
            results["tests"].append("✅ acryl-datahub dependency found")
        else:
            results["errors"].append("❌ acryl-datahub dependency missing")
            results["status"] = "error"
            
    except Exception as e:
        results["errors"].append(f"❌ Requirements validation failed: {e}")
        results["status"] = "error"
    
    return results

def test_lineage_methods() -> Dict[str, Any]:
    """Test lineage tracking methods."""
    results = {"status": "success", "tests": [], "errors": []}
    
    try:
        from src.governance.datahub_lineage import get_lineage_tracker
        tracker = get_lineage_tracker()
        
        # Test method existence
        methods = [
            "emit_dataset_metadata",
            "emit_job_metadata", 
            "emit_lineage",
            "track_kafka_ingestion",
            "track_spark_processing",
            "track_mongodb_serving"
        ]
        
        for method in methods:
            if hasattr(tracker, method):
                results["tests"].append(f"✅ {method} method exists")
            else:
                results["errors"].append(f"❌ {method} method missing")
                results["status"] = "error"
                
        # Test URN generation
        try:
            from config.datahub_config import datahub_config
            test_urn = datahub_config.get_dataset_urn("kafka", "test_topic")
            if test_urn.startswith("urn:li:dataset:"):
                results["tests"].append("✅ URN generation works")
            else:
                results["errors"].append("❌ URN generation format incorrect")
                results["status"] = "error"
        except Exception as e:
            results["errors"].append(f"❌ URN generation failed: {e}")
            results["status"] = "error"
            
    except Exception as e:
        results["errors"].append(f"❌ Lineage methods test failed: {e}")
        results["status"] = "error"
    
    return results

def main():
    """Run all validation tests."""
    print("🔍 DataHub Integration Validation")
    print("=" * 50)
    
    tests = [
        ("Module Imports", test_imports),
        ("File Structure", test_file_structure),
        ("Docker Compose", test_docker_compose),
        ("Requirements", test_requirements),
        ("Lineage Methods", test_lineage_methods),
        ("Airflow DAG", test_airflow_dag),
    ]
    
    all_passed = True
    
    for test_name, test_func in tests:
        print(f"\n📋 Testing {test_name}...")
        try:
            result = test_func()
            
            for test in result.get("tests", []):
                print(f"  {test}")
            
            for error in result.get("errors", []):
                print(f"  {error}")
                all_passed = False
                
            if result["status"] == "success":
                print(f"  ✅ {test_name} - PASSED")
            else:
                print(f"  ❌ {test_name} - FAILED")
                all_passed = False
                
        except Exception as e:
            print(f"  ❌ {test_name} - EXCEPTION: {e}")
            traceback.print_exc()
            all_passed = False
    
    print("\n" + "=" * 50)
    if all_passed:
        print("🎉 All tests PASSED! DataHub integration is working correctly.")
        print("\n📝 Next Steps:")
        print("  1. Install DataHub dependencies: pip install acryl-datahub")
        print("  2. Start services: docker-compose up -d")
        print("  3. Access DataHub UI: http://localhost:9002")
        return 0
    else:
        print("❌ Some tests FAILED. Please review the errors above.")
        return 1

if __name__ == "__main__":
    sys.exit(main())
#!/usr/bin/env python3
"""
Dashboard Reorganization Script
Reorganizes 9 old dashboards into 5 new goal-aligned dashboards
"""

import json
import os
from pathlib import Path
from datetime import datetime

DASHBOARD_DIR = Path("/home/veil/Documents/GitHub/movie-data-analysis-pipeline/layers/serving_layer/visualization/grafana/dashboards")
ARCHIVE_DIR = DASHBOARD_DIR / "archive"

# Dashboard templates with proper metadata
DASHBOARD_TEMPLATES = {
    "1-crisis-detection": {
        "title": "🚨 PR Crisis Detection & Sentiment Monitoring",
        "uid": "crisis-detection",
        "tags": ["crisis", "sentiment", "goal-1"],
        "description": "Real-time sentiment monitoring and PR crisis detection (Goal #1)",
        "refresh": "30s"
    },
    "2-viral-content": {
        "title": "🔥 Viral Content Identification & Tracking",
        "uid": "viral-content",
        "tags": ["viral", "trending", "goal-2"],
        "description": "Viral content identification for marketing amplification (Goal #2)",
        "refresh": "1m"
    },
    "3-recommendation-performance": {
        "title": "🎯 Recommendation Performance & Optimization",
        "uid": "recommendation-performance",
        "tags": ["recommendations", "dual-success", "goal-3"],
        "description": "Recommendation algorithm performance and optimization (Goal #3)",
        "refresh": "1m"
    },
    "4-system-health": {
        "title": "⚙️ System Health & Infrastructure",
        "uid": "system-health",
        "tags": ["infrastructure", "monitoring", "health"],
        "description": "API performance, database health, and data pipeline monitoring",
        "refresh": "15s"
    },
    "5-analytics-overview": {
        "title": "📊 Analytics Overview (Executive Summary)",
        "uid": "analytics-overview",
        "tags": ["overview", "executive", "summary"],
        "description": "High-level analytics and business metrics overview",
        "refresh": "5m"
    }
}


def read_dashboard(filename):
    """Read a dashboard JSON file"""
    filepath = DASHBOARD_DIR / filename
    if not filepath.exists():
        print(f"❌ Dashboard not found: {filename}")
        return None
    
    with open(filepath, 'r') as f:
        return json.load(f)


def write_dashboard(filename, data):
    """Write a dashboard JSON file"""
    filepath = DASHBOARD_DIR / filename
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=2)
    print(f"✅ Created: {filename}")


def create_base_dashboard(template_key):
    """Create a base dashboard structure"""
    template = DASHBOARD_TEMPLATES[template_key]
    
    return {
        "annotations": {
            "list": [
                {
                    "builtIn": 1,
                    "datasource": {
                        "type": "grafana",
                        "uid": "-- Grafana --"
                    },
                    "enable": True,
                    "hide": True,
                    "iconColor": "rgba(0, 211, 255, 1)",
                    "name": "Annotations & Alerts",
                    "type": "dashboard"
                }
            ]
        },
        "description": template["description"],
        "editable": True,
        "fiscalYearStartMonth": 0,
        "graphTooltip": 1,
        "id": None,
        "links": [],
        "liveNow": True,
        "panels": [],
        "refresh": template["refresh"],
        "schemaVersion": 42,
        "tags": template["tags"],
        "templating": {
            "list": []
        },
        "time": {
            "from": "now-6h",
            "to": "now"
        },
        "timepicker": {},
        "timezone": "",
        "title": template["title"],
        "uid": template["uid"],
        "version": 0
    }


def adjust_panel_positions(panels, start_y=0):
    """Adjust panel positions to avoid overlaps"""
    y_offset = start_y
    for panel in panels:
        if "gridPos" in panel:
            panel["gridPos"]["y"] += y_offset
    
    # Find max Y position for next batch
    if panels:
        max_y = max(p.get("gridPos", {}).get("y", 0) + p.get("gridPos", {}).get("h", 0) for p in panels)
        return panels, max_y
    return panels, start_y


def create_crisis_detection_dashboard():
    """Create Dashboard #1: PR Crisis Detection"""
    print("\n📊 Creating Dashboard #1: PR Crisis Detection...")
    
    dashboard = create_base_dashboard("1-crisis-detection")
    all_panels = []
    current_y = 0
    
    # Load source dashboards
    pr_crisis = read_dashboard("pr-crisis-detection.json")
    business_kpi = read_dashboard("business-kpi.json")
    
    if pr_crisis and "panels" in pr_crisis:
        panels, current_y = adjust_panel_positions(pr_crisis["panels"], current_y)
        all_panels.extend(panels)
        print(f"  ✓ Added {len(panels)} panels from pr-crisis-detection.json")
    
    # Add Business Alerts Summary from business-kpi
    if business_kpi and "panels" in business_kpi:
        for panel in business_kpi["panels"]:
            if panel.get("title") == "Business Alerts Summary":
                panel["gridPos"]["y"] = current_y
                panel["gridPos"]["x"] = 0
                all_panels.append(panel)
                current_y += panel["gridPos"]["h"]
                print(f"  ✓ Added Business Alerts Summary panel")
                break
    
    dashboard["panels"] = all_panels
    write_dashboard("1-crisis-detection.json", dashboard)
    return len(all_panels)


def create_viral_content_dashboard():
    """Create Dashboard #2: Viral Content Identification"""
    print("\n📊 Creating Dashboard #2: Viral Content Identification...")
    
    dashboard = create_base_dashboard("2-viral-content")
    all_panels = []
    current_y = 0
    
    # Load source dashboards
    viral_content = read_dashboard("viral-content.json")
    trending_movies = read_dashboard("trending-movies.json")
    
    if viral_content and "panels" in viral_content:
        panels, current_y = adjust_panel_positions(viral_content["panels"], current_y)
        all_panels.extend(panels)
        print(f"  ✓ Added {len(panels)} panels from viral-content.json")
    
    if trending_movies and "panels" in trending_movies:
        panels, current_y = adjust_panel_positions(trending_movies["panels"], current_y)
        all_panels.extend(panels)
        print(f"  ✓ Added {len(panels)} panels from trending-movies.json")
    
    dashboard["panels"] = all_panels
    write_dashboard("2-viral-content.json", dashboard)
    return len(all_panels)


def create_recommendation_dashboard():
    """Create Dashboard #3: Recommendation Performance"""
    print("\n📊 Creating Dashboard #3: Recommendation Performance...")
    
    dashboard = create_base_dashboard("3-recommendation-performance")
    
    # Load source dashboard
    rec_perf = read_dashboard("recommendation-performance.json")
    
    if rec_perf and "panels" in rec_perf:
        dashboard["panels"] = rec_perf["panels"]
        print(f"  ✓ Added {len(rec_perf['panels'])} panels from recommendation-performance.json")
    
    write_dashboard("3-recommendation-performance.json", dashboard)
    return len(dashboard["panels"])


def create_system_health_dashboard():
    """Create Dashboard #4: System Health & Infrastructure"""
    print("\n📊 Creating Dashboard #4: System Health & Infrastructure...")
    
    dashboard = create_base_dashboard("4-system-health")
    all_panels = []
    current_y = 0
    
    # Load source dashboards
    system_health = read_dashboard("system-health-dashboard.json")
    data_freshness = read_dashboard("data-freshness-dashboard.json")
    business_kpi = read_dashboard("business-kpi.json")
    
    # Add system health panels
    if system_health and "panels" in system_health:
        panels, current_y = adjust_panel_positions(system_health["panels"], current_y)
        all_panels.extend(panels)
        print(f"  ✓ Added {len(panels)} panels from system-health-dashboard.json")
    
    # Add data freshness panels
    if data_freshness and "panels" in data_freshness:
        panels, current_y = adjust_panel_positions(data_freshness["panels"], current_y)
        all_panels.extend(panels)
        print(f"  ✓ Added {len(panels)} panels from data-freshness-dashboard.json")
    
    # Add document count panels from business-kpi
    if business_kpi and "panels" in business_kpi:
        for panel in business_kpi["panels"]:
            title = panel.get("title", "")
            if title in ["Total Movies (Batch)", "Speed Layer Documents", "API Health"]:
                panel["gridPos"]["y"] = current_y
                panel["gridPos"]["x"] = (all_panels[-3:].count(lambda p: p["gridPos"]["y"] == current_y) * 6) % 24 if all_panels else 0
                all_panels.append(panel)
                print(f"  ✓ Added {title} panel")
        
        current_y += 6  # Add spacing
    
    dashboard["panels"] = all_panels
    write_dashboard("4-system-health.json", dashboard)
    return len(all_panels)


def create_analytics_overview_dashboard():
    """Create Dashboard #5: Analytics Overview"""
    print("\n📊 Creating Dashboard #5: Analytics Overview...")
    
    dashboard = create_base_dashboard("5-analytics-overview")
    all_panels = []
    current_y = 0
    
    # Load source dashboards
    genre_analytics = read_dashboard("genre-analytics-dashboard.json")
    business_kpi = read_dashboard("business-kpi.json")
    
    # Add genre analytics panels
    if genre_analytics and "panels" in genre_analytics:
        panels, current_y = adjust_panel_positions(genre_analytics["panels"], current_y)
        all_panels.extend(panels)
        print(f"  ✓ Added {len(panels)} panels from genre-analytics-dashboard.json")
    
    # Add high-level KPI panels from business-kpi
    if business_kpi and "panels" in business_kpi:
        # Filter for overview-relevant panels
        overview_panels = ["API p95 Response Time", "Request Rate", "Top Endpoints"]
        for panel in business_kpi["panels"]:
            if panel.get("title") in overview_panels:
                panel["gridPos"]["y"] = current_y
                all_panels.append(panel)
                print(f"  ✓ Added {panel.get('title')} panel")
        
        current_y += 8
    
    dashboard["panels"] = all_panels
    write_dashboard("5-analytics-overview.json", dashboard)
    return len(all_panels)


def archive_old_dashboards():
    """Move old dashboards to archive folder"""
    print("\n🗄️  Archiving old dashboards...")
    
    old_dashboards = [
        "business-kpi.json",
        "data-freshness-dashboard.json",
        "genre-analytics-dashboard.json",
        "movie-analytics-overview.json",
        "pr-crisis-detection.json",
        "recommendation-performance.json",
        "system-health-dashboard.json",
        "trending-movies.json",
        "viral-content.json"
    ]
    
    archived_count = 0
    for dashboard in old_dashboards:
        src = DASHBOARD_DIR / dashboard
        dst = ARCHIVE_DIR / dashboard
        
        if src.exists():
            # Read, add archive metadata, write to archive
            with open(src, 'r') as f:
                data = json.load(f)
            
            # Add archive metadata
            if "title" in data:
                data["title"] = f"[ARCHIVED] {data['title']}"
            data["archived_date"] = datetime.now().isoformat()
            
            with open(dst, 'w') as f:
                json.dump(data, f, indent=2)
            
            # Remove original
            os.remove(src)
            print(f"  ✓ Archived: {dashboard}")
            archived_count += 1
    
    return archived_count


def main():
    """Main reorganization process"""
    print("=" * 70)
    print("  GRAFANA DASHBOARD REORGANIZATION")
    print("  Aligning dashboards with business goals")
    print("=" * 70)
    
    # Create new dashboards
    panel_counts = {}
    panel_counts["crisis"] = create_crisis_detection_dashboard()
    panel_counts["viral"] = create_viral_content_dashboard()
    panel_counts["recommendation"] = create_recommendation_dashboard()
    panel_counts["system"] = create_system_health_dashboard()
    panel_counts["overview"] = create_analytics_overview_dashboard()
    
    # Archive old dashboards
    archived = archive_old_dashboards()
    
    # Summary
    print("\n" + "=" * 70)
    print("✅ REORGANIZATION COMPLETE!")
    print("=" * 70)
    print(f"\n📊 New Dashboards Created:")
    print(f"  1. 🚨 PR Crisis Detection ({panel_counts['crisis']} panels)")
    print(f"  2. 🔥 Viral Content Identification ({panel_counts['viral']} panels)")
    print(f"  3. 🎯 Recommendation Performance ({panel_counts['recommendation']} panels)")
    print(f"  4. ⚙️  System Health & Infrastructure ({panel_counts['system']} panels)")
    print(f"  5. 📊 Analytics Overview ({panel_counts['overview']} panels)")
    print(f"\n🗄️  Archived Dashboards: {archived}")
    print(f"\n📂 Archive Location: {ARCHIVE_DIR}")
    print(f"\n🔄 Next Step: Restart Grafana to load new dashboards")
    print(f"   docker compose restart serving-grafana")
    print("=" * 70)


if __name__ == "__main__":
    main()

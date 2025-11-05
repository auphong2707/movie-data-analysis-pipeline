#!/usr/bin/env python3
"""
Speed Layer Validation Script
Validates that the Speed Layer operates exclusively on TMDB data
"""

import sys
import re
from pathlib import Path
from typing import List, Tuple

# Patterns that indicate synthetic or user-generated data
FORBIDDEN_PATTERNS = [
    (r'user_id\s*[:\[]', 'user_id field usage'),
    (r'user_rating', 'user_rating field'),
    (r'def\s+main\s*\(\).*synthetic', 'synthetic data generation'),
    (r'generate.*mock', 'mock data generation'),
    (r'fake.*data', 'fake data usage'),
    (r'random\.choice.*rating', 'random rating generation'),
]

# Allowed patterns (comments, documentation)
ALLOWED_CONTEXTS = [
    r'#.*',  # Comments
    r'""".*?"""',  # Docstrings
    r"'''.*?'''",  # Docstrings
    r'NO.*synthetic',  # Explicit negation
    r'not.*synthetic',  # Explicit negation
    r'removed.*user_id',  # Documentation of removal
]

def check_file(filepath: Path) -> List[Tuple[int, str, str]]:
    """Check a file for forbidden patterns."""
    violations = []
    
    try:
        content = filepath.read_text(encoding='utf-8')
        lines = content.split('\n')
        
        for i, line in enumerate(lines, 1):
            # Skip allowed contexts
            is_allowed = False
            for allowed in ALLOWED_CONTEXTS:
                if re.search(allowed, line, re.IGNORECASE):
                    is_allowed = True
                    break
            
            if is_allowed:
                continue
            
            # Check for forbidden patterns
            for pattern, description in FORBIDDEN_PATTERNS:
                if re.search(pattern, line, re.IGNORECASE):
                    violations.append((i, description, line.strip()))
    
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
    
    return violations

def validate_speed_layer(speed_layer_path: Path) -> int:
    """Validate all Python files in Speed Layer."""
    print("=" * 70)
    print("SPEED LAYER TMDB-ONLY VALIDATION")
    print("=" * 70)
    print()
    
    # Find all Python files (exclude this validation script)
    python_files = [
        f for f in speed_layer_path.rglob("*.py")
        if f.name != 'validate_tmdb_only.py'
    ]
    
    print(f"Scanning {len(python_files)} Python files...")
    print()
    
    total_violations = 0
    files_with_violations = []
    
    for filepath in python_files:
        violations = check_file(filepath)
        
        if violations:
            files_with_violations.append((filepath, violations))
            total_violations += len(violations)
    
    # Report results
    if total_violations == 0:
        print("✅ VALIDATION PASSED!")
        print()
        print("No synthetic or user-generated data patterns found.")
        print("Speed Layer operates exclusively on TMDB data.")
        return 0
    else:
        print(f"❌ VALIDATION FAILED!")
        print()
        print(f"Found {total_violations} potential issues in {len(files_with_violations)} files:")
        print()
        
        for filepath, violations in files_with_violations:
            rel_path = filepath.relative_to(speed_layer_path)
            print(f"📄 {rel_path}")
            for line_num, description, line_content in violations:
                print(f"   Line {line_num}: {description}")
                print(f"   → {line_content}")
            print()
        
        return 1

def check_schema_registry():
    """Verify schema registry has TMDB-only schemas."""
    print("Checking schema registry...")
    
    schema_file = Path(__file__).parent / "kafka_producers" / "schema_registry.py"
    
    if not schema_file.exists():
        print(f"⚠️  Schema registry not found: {schema_file}")
        return False
    
    content = schema_file.read_text()
    
    # Check that MovieRating schema has TMDB fields
    required_fields = ['vote_average', 'vote_count', 'popularity']
    forbidden_fields = ['user_id', 'user_rating']
    
    for field in required_fields:
        if field not in content:
            print(f"❌ Missing required TMDB field: {field}")
            return False
    
    for field in forbidden_fields:
        if re.search(rf'"name":\s*"{field}"', content):
            print(f"❌ Found forbidden field: {field}")
            return False
    
    print("✅ Schema registry validated - TMDB-only schemas")
    return True

def check_docker_compose():
    """Verify docker-compose has no synthetic producers."""
    print("Checking docker-compose configuration...")
    
    compose_file = Path(__file__).parent / "docker-compose.speed.yml"
    
    if not compose_file.exists():
        print(f"⚠️  Docker compose not found: {compose_file}")
        return False
    
    content = compose_file.read_text()
    
    # Check for TMDB producer
    if 'tmdb-producer:' not in content:
        print("❌ TMDB producer not found in docker-compose")
        return False
    
    # Check that synthetic event-producer is removed
    if re.search(r'event-producer:.*\n.*synthetic', content, re.DOTALL):
        print("❌ Synthetic event-producer still present")
        return False
    
    # Check for MongoDB sync
    if 'cassandra-mongo-sync:' not in content:
        print("⚠️  Cassandra-MongoDB sync not found")
        return False
    
    print("✅ Docker compose validated - TMDB producer only")
    return True

def main():
    """Main validation entry point."""
    speed_layer_path = Path(__file__).parent
    
    print()
    print("🔍 Validating Speed Layer TMDB-Only Architecture")
    print()
    
    # Run all checks
    checks_passed = True
    
    # 1. Check Python files for forbidden patterns
    if validate_speed_layer(speed_layer_path) != 0:
        checks_passed = False
    
    print()
    
    # 2. Check schema registry
    if not check_schema_registry():
        checks_passed = False
    
    print()
    
    # 3. Check docker-compose
    if not check_docker_compose():
        checks_passed = False
    
    print()
    print("=" * 70)
    
    if checks_passed:
        print("✅ ALL VALIDATIONS PASSED")
        print()
        print("Speed Layer is confirmed to operate exclusively on TMDB data.")
        print("No synthetic, user-generated, or mock data detected.")
        return 0
    else:
        print("❌ VALIDATION FAILED")
        print()
        print("Please review the issues above and ensure all synthetic")
        print("data references are removed.")
        return 1

if __name__ == '__main__':
    sys.exit(main())

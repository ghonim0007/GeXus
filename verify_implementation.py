#!/usr/bin/env python3
"""
Verification script for FlatGeobuf reader implementation.

This script verifies that all components are properly implemented and can be imported.
"""

import sys
import traceback
from typing import List, Tuple

def test_imports() -> List[Tuple[str, bool, str]]:
    """Test all imports and return results."""
    results = []
    
    # Test core imports
    try:
        from gexus.core.processor import GeoProcessor
        results.append(("GeoProcessor", True, "✅ Core processor imported successfully"))
    except Exception as e:
        results.append(("GeoProcessor", False, f"❌ Failed to import GeoProcessor: {e}"))
    
    # Test base format imports
    try:
        from gexus.formats.base import BaseFormatReader, BaseFormatTransform
        results.append(("Base Format Classes", True, "✅ Base format classes imported successfully"))
    except Exception as e:
        results.append(("Base Format Classes", False, f"❌ Failed to import base classes: {e}"))
    
    # Test FlatGeobuf imports
    try:
        from gexus.formats.flatgeobuf import (
            FlatGeobufReader, 
            FlatGeobufTransform, 
            ReadFlatGeobufFn,
            SplittableFlatGeobufFn
        )
        results.append(("FlatGeobuf Classes", True, "✅ FlatGeobuf classes imported successfully"))
    except Exception as e:
        results.append(("FlatGeobuf Classes", False, f"❌ Failed to import FlatGeobuf classes: {e}"))
    
    # Test spatial transforms
    try:
        from gexus.transforms.spatial import (
            SpatialFilterTransform,
            GeometryValidationTransform,
            CRSTransformTransform,
            BoundsCalculationTransform
        )
        results.append(("Spatial Transforms", True, "✅ Spatial transforms imported successfully"))
    except Exception as e:
        results.append(("Spatial Transforms", False, f"❌ Failed to import spatial transforms: {e}"))
    
    # Test main package imports
    try:
        import gexus
        from gexus import (
            GeoProcessor,
            FlatGeobufReader,
            FlatGeobufTransform,
            SpatialFilterTransform
        )
        results.append(("Main Package", True, "✅ Main package imports working"))
    except Exception as e:
        results.append(("Main Package", False, f"❌ Failed to import from main package: {e}"))
    
    # Test Apache Beam integration
    try:
        import apache_beam as beam
        results.append(("Apache Beam", True, "✅ Apache Beam available"))
    except Exception as e:
        results.append(("Apache Beam", False, f"❌ Apache Beam not available: {e}"))
    
    return results

def test_class_instantiation() -> List[Tuple[str, bool, str]]:
    """Test basic class instantiation."""
    results = []
    
    # Test GeoProcessor
    try:
        from gexus.core.processor import GeoProcessor
        processor = GeoProcessor()
        pipeline = processor.create_pipeline()
        results.append(("GeoProcessor Instantiation", True, "✅ GeoProcessor creates pipelines successfully"))
    except Exception as e:
        results.append(("GeoProcessor Instantiation", False, f"❌ GeoProcessor instantiation failed: {e}"))
    
    # Test FlatGeobufTransform
    try:
        from gexus.formats.flatgeobuf import FlatGeobufTransform
        transform = FlatGeobufTransform("*.fgb")
        results.append(("FlatGeobufTransform Instantiation", True, "✅ FlatGeobufTransform instantiated successfully"))
    except Exception as e:
        results.append(("FlatGeobufTransform Instantiation", False, f"❌ FlatGeobufTransform instantiation failed: {e}"))
    
    # Test spatial transforms
    try:
        from gexus.transforms.spatial import SpatialFilterTransform
        transform = SpatialFilterTransform(bbox=(-180, -90, 180, 90))
        results.append(("SpatialFilterTransform Instantiation", True, "✅ SpatialFilterTransform instantiated successfully"))
    except Exception as e:
        results.append(("SpatialFilterTransform Instantiation", False, f"❌ SpatialFilterTransform instantiation failed: {e}"))
    
    return results

def test_dependencies() -> List[Tuple[str, bool, str]]:
    """Test external dependencies."""
    results = []
    
    # Test optional dependencies (these might not be installed)
    optional_deps = [
        ("flatgeobuf", "FlatGeobuf library"),
        ("shapely", "Shapely geometry library"),
        ("pyproj", "PyProj CRS library")
    ]
    
    for dep_name, dep_desc in optional_deps:
        try:
            __import__(dep_name)
            results.append((dep_desc, True, f"✅ {dep_desc} available"))
        except ImportError:
            results.append((dep_desc, False, f"⚠️  {dep_desc} not installed (optional for basic testing)"))
    
    return results

def print_results(results: List[Tuple[str, bool, str]], title: str):
    """Print test results in a formatted way."""
    print(f"\n{'='*60}")
    print(f"{title}")
    print(f"{'='*60}")
    
    passed = 0
    total = len(results)
    
    for name, success, message in results:
        print(f"{message}")
        if success:
            passed += 1
    
    print(f"\n📊 Results: {passed}/{total} tests passed")
    return passed, total

def main():
    """Run all verification tests."""
    print("🔍 GeXus FlatGeobuf Implementation Verification")
    print("=" * 60)
    
    total_passed = 0
    total_tests = 0
    
    # Test imports
    import_results = test_imports()
    passed, tests = print_results(import_results, "📦 Import Tests")
    total_passed += passed
    total_tests += tests
    
    # Test instantiation
    instantiation_results = test_class_instantiation()
    passed, tests = print_results(instantiation_results, "🏗️  Instantiation Tests")
    total_passed += passed
    total_tests += tests
    
    # Test dependencies
    dependency_results = test_dependencies()
    passed, tests = print_results(dependency_results, "📚 Dependency Tests")
    total_passed += passed
    total_tests += tests
    
    # Final summary
    print(f"\n{'='*60}")
    print(f"🎯 FINAL SUMMARY")
    print(f"{'='*60}")
    print(f"Total Tests: {total_tests}")
    print(f"Passed: {total_passed}")
    print(f"Failed: {total_tests - total_passed}")
    print(f"Success Rate: {(total_passed/total_tests)*100:.1f}%")
    
    if total_passed == total_tests:
        print("\n🎉 ALL TESTS PASSED! Implementation is ready.")
        return 0
    else:
        print(f"\n⚠️  {total_tests - total_passed} tests failed. Check the output above for details.")
        return 1

if __name__ == "__main__":
    sys.exit(main())

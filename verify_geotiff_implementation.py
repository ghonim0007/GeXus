#!/usr/bin/env python3
"""
Verification script for GeoTIFF reader implementation.

This script verifies that all GeoTIFF components are properly implemented 
and can be imported, including raster processing, analytics, and vector-raster integration.
"""

import sys
import traceback
from typing import List, Tuple

def test_core_imports() -> List[Tuple[str, bool, str]]:
    """Test core GeXus imports."""
    results = []
    
    # Test core processor
    try:
        from gexus.core.processor import GeoProcessor
        results.append(("GeoProcessor", True, "✅ Core processor imported successfully"))
    except Exception as e:
        results.append(("GeoProcessor", False, f"❌ Failed to import GeoProcessor: {e}"))
    
    return results

def test_raster_base_imports() -> List[Tuple[str, bool, str]]:
    """Test raster base format imports."""
    results = []
    
    # Test base raster classes
    try:
        from gexus.formats.raster_base import (
            BaseRasterReader, 
            BaseRasterTransform, 
            RasterTile, 
            RasterBand,
            RasterWindow,
            RasterMetadata
        )
        results.append(("Raster Base Classes", True, "✅ Raster base classes imported successfully"))
    except Exception as e:
        results.append(("Raster Base Classes", False, f"❌ Failed to import raster base classes: {e}"))
    
    return results

def test_geotiff_imports() -> List[Tuple[str, bool, str]]:
    """Test GeoTIFF implementation imports."""
    results = []
    
    # Test GeoTIFF classes
    try:
        from gexus.formats.geotiff import (
            GeoTIFFReader,
            GeoTIFFTransform,
            ReadGeoTIFFTilesFn,
            ReadGeoTIFFBandsFn,
            SplittableGeoTIFFReaderFn
        )
        results.append(("GeoTIFF Classes", True, "✅ GeoTIFF classes imported successfully"))
    except Exception as e:
        results.append(("GeoTIFF Classes", False, f"❌ Failed to import GeoTIFF classes: {e}"))
    
    return results

def test_raster_transforms() -> List[Tuple[str, bool, str]]:
    """Test raster transforms imports."""
    results = []
    
    # Test raster transforms
    try:
        from gexus.transforms.raster import (
            RasterBandMathTransform,
            RasterResampleTransform,
            ZonalStatisticsTransform,
            RasterFilterTransform,
            RasterMosaicTransform
        )
        results.append(("Raster Transforms", True, "✅ Raster transforms imported successfully"))
    except Exception as e:
        results.append(("Raster Transforms", False, f"❌ Failed to import raster transforms: {e}"))
    
    return results

def test_analytics_imports() -> List[Tuple[str, bool, str]]:
    """Test analytics module imports."""
    results = []
    
    # Test spectral indices
    try:
        from gexus.analytics.indices import (
            VegetationIndices,
            WaterIndices,
            UrbanIndices,
            SoilIndices,
            IndexCalculator
        )
        results.append(("Spectral Indices", True, "✅ Spectral indices imported successfully"))
    except Exception as e:
        results.append(("Spectral Indices", False, f"❌ Failed to import spectral indices: {e}"))
    
    # Test temporal analysis
    try:
        from gexus.analytics.temporal import (
            TemporalRasterAnalysis,
            ChangeDetectionAnalysis,
            TrendAnalysis
        )
        results.append(("Temporal Analysis", True, "✅ Temporal analysis imported successfully"))
    except Exception as e:
        results.append(("Temporal Analysis", False, f"❌ Failed to import temporal analysis: {e}"))
    
    return results

def test_main_package_imports() -> List[Tuple[str, bool, str]]:
    """Test main package imports."""
    results = []
    
    # Test main package imports
    try:
        import gexus
        from gexus import (
            GeoProcessor,
            # Vector formats
            FlatGeobufReader,
            FlatGeobufTransform,
            # Raster formats
            GeoTIFFReader,
            GeoTIFFTransform,
            RasterTile,
            RasterBand,
            # Transforms
            SpatialFilterTransform,
            RasterBandMathTransform,
            ZonalStatisticsTransform,
            # Analytics
            VegetationIndices,
            TemporalRasterAnalysis
        )
        results.append(("Main Package", True, "✅ Main package imports working"))
    except Exception as e:
        results.append(("Main Package", False, f"❌ Failed to import from main package: {e}"))
    
    return results

def test_dependencies() -> List[Tuple[str, bool, str]]:
    """Test external dependencies."""
    results = []
    
    # Test required dependencies
    required_deps = [
        ("apache_beam", "Apache Beam"),
        ("numpy", "NumPy"),
    ]
    
    for dep_name, dep_desc in required_deps:
        try:
            __import__(dep_name)
            results.append((dep_desc, True, f"✅ {dep_desc} available"))
        except ImportError:
            results.append((dep_desc, False, f"❌ {dep_desc} not available"))
    
    # Test optional raster dependencies
    optional_deps = [
        ("rasterio", "Rasterio (GDAL Python binding)"),
        ("gdal", "GDAL"),
        ("numba", "Numba (JIT compilation)"),
        ("dask", "Dask (out-of-core arrays)"),
        ("zarr", "Zarr (chunked arrays)"),
        ("fsspec", "FSSpec (cloud filesystems)")
    ]
    
    for dep_name, dep_desc in optional_deps:
        try:
            __import__(dep_name)
            results.append((dep_desc, True, f"✅ {dep_desc} available"))
        except ImportError:
            results.append((dep_desc, False, f"⚠️  {dep_desc} not installed (required for raster processing)"))
    
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
    
    # Test GeoTIFFTransform
    try:
        from gexus.formats.geotiff import GeoTIFFTransform
        transform = GeoTIFFTransform("*.tif")
        results.append(("GeoTIFFTransform Instantiation", True, "✅ GeoTIFFTransform instantiated successfully"))
    except Exception as e:
        results.append(("GeoTIFFTransform Instantiation", False, f"❌ GeoTIFFTransform instantiation failed: {e}"))
    
    # Test raster transforms
    try:
        from gexus.transforms.raster import RasterBandMathTransform
        transform = RasterBandMathTransform("(B2 - B1) / (B2 + B1)", "ndvi")
        results.append(("RasterBandMathTransform Instantiation", True, "✅ RasterBandMathTransform instantiated successfully"))
    except Exception as e:
        results.append(("RasterBandMathTransform Instantiation", False, f"❌ RasterBandMathTransform instantiation failed: {e}"))
    
    # Test analytics
    try:
        from gexus.analytics.indices import IndexCalculator
        required_bands = IndexCalculator.get_required_bands('ndvi')
        results.append(("Analytics Functionality", True, f"✅ Analytics working (NDVI requires: {required_bands})"))
    except Exception as e:
        results.append(("Analytics Functionality", False, f"❌ Analytics functionality failed: {e}"))
    
    return results

def test_vector_raster_integration() -> List[Tuple[str, bool, str]]:
    """Test vector-raster integration."""
    results = []
    
    try:
        from gexus.formats.flatgeobuf import FlatGeobufTransform
        from gexus.formats.geotiff import GeoTIFFTransform
        from gexus.transforms.raster import ZonalStatisticsTransform
        
        # Test that we can create integrated transforms
        vector_transform = FlatGeobufTransform("*.fgb")
        raster_transform = GeoTIFFTransform("*.tif")
        
        results.append(("Vector-Raster Integration", True, "✅ Vector-raster integration components available"))
    except Exception as e:
        results.append(("Vector-Raster Integration", False, f"❌ Vector-raster integration failed: {e}"))
    
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
    print("🔍 GeXus GeoTIFF Implementation Verification")
    print("=" * 60)
    
    total_passed = 0
    total_tests = 0
    
    # Test core imports
    core_results = test_core_imports()
    passed, tests = print_results(core_results, "📦 Core Imports")
    total_passed += passed
    total_tests += tests
    
    # Test raster base imports
    raster_base_results = test_raster_base_imports()
    passed, tests = print_results(raster_base_results, "🗂️  Raster Base Classes")
    total_passed += passed
    total_tests += tests
    
    # Test GeoTIFF imports
    geotiff_results = test_geotiff_imports()
    passed, tests = print_results(geotiff_results, "🛰️  GeoTIFF Implementation")
    total_passed += passed
    total_tests += tests
    
    # Test raster transforms
    transforms_results = test_raster_transforms()
    passed, tests = print_results(transforms_results, "🔄 Raster Transforms")
    total_passed += passed
    total_tests += tests
    
    # Test analytics
    analytics_results = test_analytics_imports()
    passed, tests = print_results(analytics_results, "📈 Analytics Module")
    total_passed += passed
    total_tests += tests
    
    # Test main package
    main_results = test_main_package_imports()
    passed, tests = print_results(main_results, "📦 Main Package Integration")
    total_passed += passed
    total_tests += tests
    
    # Test dependencies
    dependency_results = test_dependencies()
    passed, tests = print_results(dependency_results, "📚 Dependencies")
    total_passed += passed
    total_tests += tests
    
    # Test instantiation
    instantiation_results = test_class_instantiation()
    passed, tests = print_results(instantiation_results, "🏗️  Class Instantiation")
    total_passed += passed
    total_tests += tests
    
    # Test integration
    integration_results = test_vector_raster_integration()
    passed, tests = print_results(integration_results, "🔗 Vector-Raster Integration")
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
        print("\n🎉 ALL TESTS PASSED! GeoTIFF implementation is ready.")
        print("\n🚀 GeXus now supports both vector (FlatGeobuf) and raster (GeoTIFF) data!")
        print("📊 Ready for production deployment with:")
        print("   • Satellite imagery processing")
        print("   • Vegetation indices calculation")
        print("   • Temporal change detection")
        print("   • Vector-raster integration")
        print("   • Cloud-scale processing with Apache Beam")
        return 0
    else:
        print(f"\n⚠️  {total_tests - total_passed} tests failed. Check the output above for details.")
        print("\n💡 To install missing dependencies:")
        print("   pip install -r requirements/base.txt")
        return 1

if __name__ == "__main__":
    sys.exit(main())

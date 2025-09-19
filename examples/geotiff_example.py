#!/usr/bin/env python3
"""
GeoTIFF Reader Example for GeXus Framework

This example demonstrates how to use the GeoTIFF reader with Apache Beam
for scalable raster data processing, including satellite imagery analysis,
vegetation indices calculation, and vector-raster integration.
"""

import json
import logging
from typing import Dict, Any
import numpy as np

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from gexus.core.processor import GeoProcessor
from gexus.formats.geotiff import GeoTIFFTransform
from gexus.formats.flatgeobuf import FlatGeobufTransform
from gexus.transforms.raster import (
    RasterBandMathTransform,
    RasterResampleTransform,
    ZonalStatisticsTransform,
    RasterFilterTransform,
    RasterMosaicTransform
)
from gexus.analytics.indices import IndexCalculator, VegetationIndices
from gexus.analytics.temporal import TemporalRasterAnalysis, ChangeDetectionAnalysis

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def process_raster_tile(tile) -> Dict[str, Any]:
    """Process a single raster tile with custom logic.
    
    Args:
        tile: RasterTile object
        
    Returns:
        Processed tile dictionary with statistics
    """
    try:
        # Calculate basic statistics
        stats = {
            'tile_id': tile.tile_id,
            'shape': tile.data.shape,
            'bands': len(tile.bands),
            'mean_values': [float(np.nanmean(tile.data[i])) for i in range(tile.data.shape[0])],
            'std_values': [float(np.nanstd(tile.data[i])) for i in range(tile.data.shape[0])],
            'valid_pixels': int(np.sum(~np.isnan(tile.data))),
            'nodata_pixels': int(np.sum(np.isnan(tile.data)))
        }
        
        # Add spatial information
        if hasattr(tile, 'window'):
            stats['spatial_bounds'] = {
                'col_off': tile.window.col_off,
                'row_off': tile.window.row_off,
                'width': tile.window.width,
                'height': tile.window.height
            }
        
        # Add processing timestamp
        import datetime
        stats['processed_at'] = datetime.datetime.now().isoformat()
        
        return stats
        
    except Exception as e:
        logger.error(f"Failed to process tile {tile.tile_id}: {e}")
        return {
            'tile_id': getattr(tile, 'tile_id', 'unknown'),
            'error': str(e),
            'processed_at': datetime.datetime.now().isoformat()
        }


def calculate_vegetation_indices(tile):
    """Calculate vegetation indices for a raster tile.
    
    Args:
        tile: RasterTile with at least 4 bands (RGB + NIR)
        
    Returns:
        Dictionary with calculated indices
    """
    try:
        if tile.data.shape[0] < 4:
            return {
                'tile_id': tile.tile_id,
                'error': 'Insufficient bands for vegetation indices'
            }
        
        # Extract bands (assuming Landsat-like band order)
        bands = {
            'red': tile.data[2],      # Band 3 (Red)
            'green': tile.data[1],    # Band 2 (Green)
            'blue': tile.data[0],     # Band 1 (Blue)
            'nir': tile.data[3]       # Band 4 (NIR)
        }
        
        # Calculate indices
        indices = {}
        try:
            indices['ndvi'] = IndexCalculator.calculate('ndvi', bands)
            indices['gndvi'] = IndexCalculator.calculate('gndvi', bands)
        except Exception as e:
            logger.warning(f"Failed to calculate indices for {tile.tile_id}: {e}")
            return {'tile_id': tile.tile_id, 'error': str(e)}
        
        # Calculate statistics for each index
        result = {'tile_id': tile.tile_id}
        for index_name, index_data in indices.items():
            valid_data = index_data[~np.isnan(index_data)]
            if len(valid_data) > 0:
                result[f'{index_name}_mean'] = float(np.mean(valid_data))
                result[f'{index_name}_std'] = float(np.std(valid_data))
                result[f'{index_name}_min'] = float(np.min(valid_data))
                result[f'{index_name}_max'] = float(np.max(valid_data))
                result[f'{index_name}_valid_pixels'] = len(valid_data)
            else:
                result[f'{index_name}_mean'] = None
                result[f'{index_name}_std'] = None
                result[f'{index_name}_min'] = None
                result[f'{index_name}_max'] = None
                result[f'{index_name}_valid_pixels'] = 0
        
        return result
        
    except Exception as e:
        logger.error(f"Vegetation indices calculation failed for {tile.tile_id}: {e}")
        return {
            'tile_id': tile.tile_id,
            'error': str(e)
        }


def filter_high_vegetation(tile):
    """Filter tiles with high vegetation content.
    
    Args:
        tile: RasterTile object
        
    Returns:
        True if tile has high vegetation content
    """
    try:
        if tile.data.shape[0] < 4:
            return False
        
        # Calculate NDVI
        red = tile.data[2].astype(np.float32)
        nir = tile.data[3].astype(np.float32)
        
        # Avoid division by zero
        denominator = nir + red
        ndvi = np.where(denominator != 0, (nir - red) / denominator, np.nan)
        
        # Check if mean NDVI > 0.3 (indicating vegetation)
        valid_ndvi = ndvi[~np.isnan(ndvi)]
        if len(valid_ndvi) > 0:
            mean_ndvi = np.mean(valid_ndvi)
            return mean_ndvi > 0.3
        
        return False
        
    except Exception as e:
        logger.warning(f"Vegetation filtering failed for {tile.tile_id}: {e}")
        return False


def example_basic_usage():
    """Example of basic GeoTIFF reader usage."""
    logger.info("=== Basic Usage Example ===")
    
    # Create processor and pipeline
    processor = GeoProcessor()
    pipeline = processor.create_pipeline()
    
    # Read GeoTIFF data
    tile_stats = (
        pipeline
        | 'Read GeoTIFF' >> GeoTIFFTransform('data/*.tif')
        | 'Process Tiles' >> beam.Map(process_raster_tile)
        | 'Log Stats' >> beam.Map(lambda stats: logger.info(f"Processed: {stats['tile_id']}") or stats)
    )
    
    # Run pipeline
    result = pipeline.run()
    result.wait_until_finish()
    logger.info("Basic usage example completed")


def example_satellite_imagery_processing():
    """Example of satellite imagery processing with vegetation indices."""
    logger.info("=== Satellite Imagery Processing Example ===")
    
    processor = GeoProcessor()
    pipeline = processor.create_pipeline()
    
    # Process Landsat imagery
    vegetation_analysis = (
        pipeline
        | 'Read Landsat' >> GeoTIFFTransform(
            'landsat/*.tif',
            bands=[1, 2, 3, 4],  # Blue, Green, Red, NIR
            tile_size=(1024, 1024)
        )
        | 'Calculate Vegetation Indices' >> beam.Map(calculate_vegetation_indices)
        | 'Filter Valid Results' >> beam.Filter(lambda x: 'error' not in x)
        | 'Log Vegetation Stats' >> beam.Map(
            lambda stats: logger.info(
                f"Tile {stats['tile_id']}: NDVI={stats.get('ndvi_mean', 'N/A'):.3f}"
            ) or stats
        )
    )
    
    result = pipeline.run()
    result.wait_until_finish()
    logger.info("Satellite imagery processing example completed")


def example_band_math_operations():
    """Example of band math operations for spectral analysis."""
    logger.info("=== Band Math Operations Example ===")
    
    processor = GeoProcessor()
    pipeline = processor.create_pipeline()
    
    # Calculate NDVI using band math transform
    ndvi_analysis = (
        pipeline
        | 'Read Satellite Data' >> GeoTIFFTransform('satellite.tif')
        | 'Calculate NDVI' >> RasterBandMathTransform(
            expression="(B4 - B3) / (B4 + B3)",  # NIR - Red / NIR + Red
            output_name="ndvi"
        )
        | 'Filter High Vegetation' >> beam.Filter(filter_high_vegetation)
        | 'Process NDVI Tiles' >> beam.Map(process_raster_tile)
        | 'Log NDVI Results' >> beam.Map(
            lambda stats: logger.info(f"High vegetation tile: {stats['tile_id']}") or stats
        )
    )
    
    result = pipeline.run()
    result.wait_until_finish()
    logger.info("Band math operations example completed")


def example_vector_raster_integration():
    """Example of vector-raster integration with zonal statistics."""
    logger.info("=== Vector-Raster Integration Example ===")
    
    processor = GeoProcessor()
    pipeline = processor.create_pipeline()
    
    # Read vector boundaries
    vector_boundaries = pipeline | 'Read Boundaries' >> FlatGeobufTransform('boundaries.fgb')
    
    # Read raster data
    raster_tiles = pipeline | 'Read Land Cover' >> GeoTIFFTransform('land_cover.tif')
    
    # Calculate zonal statistics
    zonal_stats = raster_tiles | 'Calculate Zonal Stats' >> ZonalStatisticsTransform(
        vector_boundaries=vector_boundaries,
        stats=['mean', 'std', 'count', 'min', 'max']
    )
    
    # Log results
    logged_stats = zonal_stats | 'Log Zonal Stats' >> beam.Map(
        lambda stats: logger.info(
            f"Region {stats.get('feature_id', 'unknown')}: "
            f"mean={stats.get('mean', 'N/A')}, count={stats.get('count', 'N/A')}"
        ) or stats
    )
    
    result = pipeline.run()
    result.wait_until_finish()
    logger.info("Vector-raster integration example completed")


def example_temporal_analysis():
    """Example of temporal analysis with change detection."""
    logger.info("=== Temporal Analysis Example ===")
    
    processor = GeoProcessor()
    pipeline = processor.create_pipeline()
    
    # Read before and after images
    before_tiles = pipeline | 'Read Before' >> GeoTIFFTransform('before/*.tif')
    after_tiles = pipeline | 'Read After' >> GeoTIFFTransform('after/*.tif')
    
    # Pair tiles by spatial location
    def key_by_location(tile):
        # Extract spatial key from tile (simplified)
        return (tile.tile_id.split('_')[0], tile)
    
    before_keyed = before_tiles | 'Key Before' >> beam.Map(key_by_location)
    after_keyed = after_tiles | 'Key After' >> beam.Map(key_by_location)
    
    # Combine and detect changes
    def detect_changes(location_data):
        location, data_dict = location_data
        before_tiles = list(data_dict['before'])
        after_tiles = list(data_dict['after'])
        
        if len(before_tiles) != 1 or len(after_tiles) != 1:
            return {'location': location, 'error': 'Data mismatch'}
        
        before_data = before_tiles[0].data.astype(np.float32)
        after_data = after_tiles[0].data.astype(np.float32)
        
        # Simple change detection
        change = np.abs(after_data - before_data)
        change_magnitude = np.mean(change)
        
        return {
            'location': location,
            'change_magnitude': float(change_magnitude),
            'significant_change': change_magnitude > 100
        }
    
    change_results = (
        ({'before': before_keyed, 'after': after_keyed})
        | 'Group by Location' >> beam.CoGroupByKey()
        | 'Detect Changes' >> beam.Map(detect_changes)
        | 'Log Changes' >> beam.Map(
            lambda result: logger.info(
                f"Location {result.get('location', 'unknown')}: "
                f"change={result.get('change_magnitude', 'N/A'):.2f}, "
                f"significant={result.get('significant_change', False)}"
            ) or result
        )
    )
    
    result = pipeline.run()
    result.wait_until_finish()
    logger.info("Temporal analysis example completed")


def example_raster_mosaicking():
    """Example of raster mosaicking for large area analysis."""
    logger.info("=== Raster Mosaicking Example ===")
    
    processor = GeoProcessor()
    pipeline = processor.create_pipeline()
    
    # Read multiple raster tiles
    tiles = pipeline | 'Read Tiles' >> GeoTIFFTransform('tiles/*.tif')
    
    # Create mosaic
    output_bounds = (-180, -90, 180, 90)  # World bounds
    output_resolution = 0.1
    
    mosaic = tiles | 'Create Mosaic' >> RasterMosaicTransform(
        output_bounds=output_bounds,
        output_resolution=output_resolution
    )
    
    # Process mosaic
    mosaic_stats = mosaic | 'Process Mosaic' >> beam.Map(process_raster_tile)
    
    # Log results
    logged_mosaic = mosaic_stats | 'Log Mosaic' >> beam.Map(
        lambda stats: logger.info(f"Mosaic created: {stats}") or stats
    )
    
    result = pipeline.run()
    result.wait_until_finish()
    logger.info("Raster mosaicking example completed")


def example_cloud_processing():
    """Example of cloud-scale processing with Dataflow."""
    logger.info("=== Cloud Processing Example ===")
    
    # Configure Dataflow options (commented out for local testing)
    # options = PipelineOptions([
    #     '--project=your-project-id',
    #     '--region=us-central1',
    #     '--runner=DataflowRunner',
    #     '--temp_location=gs://your-bucket/temp',
    #     '--staging_location=gs://your-bucket/staging',
    #     '--max_num_workers=50'
    # ])
    
    processor = GeoProcessor()
    # pipeline = processor.create_pipeline(options)  # For Dataflow
    pipeline = processor.create_pipeline()  # For local testing
    
    # Large-scale satellite data processing
    vegetation_monitoring = (
        pipeline
        | 'Read Satellite Archive' >> GeoTIFFTransform(
            'gs://satellite-archive/*.tif',  # Cloud storage path
            bands=[3, 4],  # Red and NIR bands
            use_splittable=True,  # Enable parallel processing
            tile_size=(2048, 2048)  # Larger tiles for efficiency
        )
        | 'Calculate NDVI' >> RasterBandMathTransform(
            expression="(B2 - B1) / (B2 + B1)",
            output_name="ndvi"
        )
        | 'Filter Vegetation Areas' >> beam.Filter(filter_high_vegetation)
        | 'Calculate Statistics' >> beam.Map(calculate_vegetation_indices)
        | 'Log Results' >> beam.Map(
            lambda stats: logger.info(f"Processed vegetation area: {stats['tile_id']}") or stats
        )
    )
    
    result = pipeline.run()
    # result.wait_until_finish()  # Uncomment for Dataflow
    logger.info("Cloud processing example configured")


def example_error_handling():
    """Example of robust error handling in raster processing."""
    logger.info("=== Error Handling Example ===")
    
    def safe_process_tile(tile):
        """Safely process a tile with comprehensive error handling."""
        try:
            # Check for valid data
            if tile.data is None or tile.data.size == 0:
                return {'tile_id': tile.tile_id, 'error': 'Empty data'}
            
            # Check for all NaN data
            if np.all(np.isnan(tile.data)):
                return {'tile_id': tile.tile_id, 'error': 'All NaN data'}
            
            # Process normally
            return process_raster_tile(tile)
            
        except Exception as e:
            logger.error(f"Tile processing error: {e}")
            return {
                'tile_id': getattr(tile, 'tile_id', 'unknown'),
                'error': str(e),
                'error_type': type(e).__name__
            }
    
    def log_processing_errors(result):
        """Log processing errors and return valid results."""
        if 'error' in result:
            logger.error(f"Processing error for {result['tile_id']}: {result['error']}")
        return result
    
    processor = GeoProcessor()
    pipeline = processor.create_pipeline()
    
    processed_tiles = (
        pipeline
        | 'Read Raster Data' >> GeoTIFFTransform('data/*.tif')
        | 'Safe Process' >> beam.Map(safe_process_tile)
        | 'Log Errors' >> beam.Map(log_processing_errors)
        | 'Filter Valid Results' >> beam.Filter(lambda x: 'error' not in x)
    )
    
    result = pipeline.run()
    result.wait_until_finish()
    logger.info("Error handling example completed")


def example_performance_optimization():
    """Example of performance optimization techniques."""
    logger.info("=== Performance Optimization Example ===")
    
    processor = GeoProcessor()
    pipeline = processor.create_pipeline()
    
    # Optimized processing pipeline
    optimized_processing = (
        pipeline
        | 'Read Large Raster' >> GeoTIFFTransform(
            'large_dataset/*.tif',
            use_splittable=True,  # Enable parallel processing
            tile_size=(1024, 1024),  # Optimal tile size
            bands=[1, 2, 3, 4]  # Only read required bands
        )
        | 'Batch Process' >> beam.BatchElements(
            min_batch_size=10,  # Process tiles in batches
            max_batch_size=50
        )
        | 'Process Batches' >> beam.Map(
            lambda batch: [process_raster_tile(tile) for tile in batch]
        )
        | 'Flatten Results' >> beam.FlatMap(lambda x: x)
        | 'Cache Results' >> beam.Map(lambda x: x)  # Add caching if needed
    )
    
    result = pipeline.run()
    result.wait_until_finish()
    logger.info("Performance optimization example completed")


def main():
    """Run all examples."""
    logger.info("Starting GeoTIFF Reader Examples")
    
    # Note: These examples assume you have GeoTIFF files available
    # Comment out examples that require specific data files
    
    try:
        example_basic_usage()
    except Exception as e:
        logger.error(f"Basic usage example failed: {e}")
    
    try:
        example_satellite_imagery_processing()
    except Exception as e:
        logger.error(f"Satellite imagery example failed: {e}")
    
    try:
        example_band_math_operations()
    except Exception as e:
        logger.error(f"Band math example failed: {e}")
    
    try:
        example_vector_raster_integration()
    except Exception as e:
        logger.error(f"Vector-raster integration example failed: {e}")
    
    try:
        example_temporal_analysis()
    except Exception as e:
        logger.error(f"Temporal analysis example failed: {e}")
    
    try:
        example_raster_mosaicking()
    except Exception as e:
        logger.error(f"Raster mosaicking example failed: {e}")
    
    try:
        example_cloud_processing()
    except Exception as e:
        logger.error(f"Cloud processing example failed: {e}")
    
    try:
        example_error_handling()
    except Exception as e:
        logger.error(f"Error handling example failed: {e}")
    
    try:
        example_performance_optimization()
    except Exception as e:
        logger.error(f"Performance optimization example failed: {e}")
    
    logger.info("All examples completed")


if __name__ == '__main__':
    main()

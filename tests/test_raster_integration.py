"""Integration tests for raster processing with sample data."""

import json
import os
import tempfile
import unittest
from typing import List, Dict, Any
import numpy as np

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

try:
    import rasterio
    from rasterio.windows import Window
    from rasterio.crs import CRS
    from rasterio.transform import Affine
except ImportError:
    rasterio = None

from gexus.core.processor import GeoProcessor
from gexus.formats.geotiff import GeoTIFFTransform
from gexus.formats.flatgeobuf import FlatGeobufTransform
from gexus.formats.raster_base import RasterTile, RasterBand
from gexus.transforms.raster import (
    RasterBandMathTransform,
    RasterResampleTransform,
    ZonalStatisticsTransform,
    RasterFilterTransform,
    RasterMosaicTransform
)
from gexus.analytics.indices import VegetationIndices, IndexCalculator
from gexus.analytics.temporal import TemporalRasterAnalysis, ChangeDetectionAnalysis


class TestRasterIntegration(unittest.TestCase):
    """Integration tests for raster processing with GeXus framework."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.sample_raster_data = self._create_sample_raster_data()
        self.sample_vector_data = self._create_sample_vector_data()
    
    def tearDown(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def _create_sample_raster_data(self) -> List[RasterTile]:
        """Create sample raster tiles for testing."""
        tiles = []
        
        for i in range(2):
            for j in range(2):
                # Create multi-band data (RGB + NIR)
                data = np.random.randint(0, 1000, (4, 512, 512), dtype=np.uint16)
                
                # Make NIR band (band 4) higher values for vegetation simulation
                data[3] = data[0] + np.random.randint(200, 500, (512, 512))
                
                tile = RasterTile(
                    data=data,
                    window=Window(j * 512, i * 512, 512, 512),
                    transform=(0.1, 0, -180 + j * 51.2, 0, -0.1, 90 - i * 51.2),
                    bands=[1, 2, 3, 4],  # Red, Green, Blue, NIR
                    nodata=None,
                    tile_id=f"{i}_{j}"
                )
                tiles.append(tile)
        
        return tiles
    
    def _create_sample_vector_data(self) -> List[Dict[str, Any]]:
        """Create sample vector features for testing."""
        return [
            {
                'type': 'Feature',
                'geometry': {
                    'type': 'Polygon',
                    'coordinates': [[
                        [-179, 89], [-179, 45], [-90, 45], [-90, 89], [-179, 89]
                    ]]
                },
                'properties': {
                    'id': 'region_1',
                    'name': 'North West',
                    'area_km2': 12000
                }
            },
            {
                'type': 'Feature',
                'geometry': {
                    'type': 'Polygon',
                    'coordinates': [[
                        [-90, 89], [-90, 45], [0, 45], [0, 89], [-90, 89]
                    ]]
                },
                'properties': {
                    'id': 'region_2',
                    'name': 'North East',
                    'area_km2': 11000
                }
            }
        ]
    
    def test_geo_processor_integration(self):
        """Test integration with GeoProcessor."""
        processor = GeoProcessor()
        pipeline = processor.create_pipeline()
        
        # Verify pipeline creation
        self.assertIsNotNone(pipeline)
        self.assertIsInstance(pipeline, beam.Pipeline)
    
    def test_raster_band_math_pipeline(self):
        """Test raster band math processing pipeline."""
        with TestPipeline() as p:
            # Create raster tiles from sample data
            tiles = p | 'Create Tiles' >> beam.Create(self.sample_raster_data)
            
            # Calculate NDVI using band math
            ndvi_tiles = tiles | 'Calculate NDVI' >> RasterBandMathTransform(
                expression="(B4 - B1) / (B4 + B1)",  # NIR - Red / NIR + Red
                output_name="ndvi"
            )
            
            # Filter tiles with valid NDVI values
            valid_tiles = ndvi_tiles | 'Filter Valid NDVI' >> RasterFilterTransform(
                lambda data: np.any((data >= -1) & (data <= 1) & ~np.isnan(data))
            )
            
            # Verify results
            def check_ndvi_tiles(result):
                self.assertGreater(len(result), 0)
                for tile in result:
                    self.assertIsInstance(tile, RasterTile)
                    # Should have 1 band (NDVI result)
                    self.assertEqual(tile.data.shape[0], 1)
                    # NDVI values should be in valid range
                    valid_data = tile.data[~np.isnan(tile.data)]
                    if len(valid_data) > 0:
                        self.assertTrue(np.all(valid_data >= -1))
                        self.assertTrue(np.all(valid_data <= 1))
            
            assert_that(valid_tiles, check_ndvi_tiles)
    
    def test_vegetation_indices_calculation(self):
        """Test vegetation indices calculation."""
        with TestPipeline() as p:
            tiles = p | 'Create Tiles' >> beam.Create(self.sample_raster_data)
            
            # Extract individual bands
            def extract_bands(tile):
                """Extract bands as separate arrays."""
                return {
                    'red': tile.data[0],
                    'green': tile.data[1], 
                    'blue': tile.data[2],
                    'nir': tile.data[3],
                    'tile_id': tile.tile_id
                }
            
            band_data = tiles | 'Extract Bands' >> beam.Map(extract_bands)
            
            # Calculate multiple indices
            def calculate_indices(band_dict):
                """Calculate various vegetation indices."""
                try:
                    bands = {k: v for k, v in band_dict.items() if k != 'tile_id'}
                    
                    indices = {}
                    indices['ndvi'] = IndexCalculator.calculate('ndvi', bands)
                    indices['gndvi'] = IndexCalculator.calculate('gndvi', bands)
                    indices['tile_id'] = band_dict['tile_id']
                    
                    return indices
                except Exception as e:
                    return {'error': str(e), 'tile_id': band_dict.get('tile_id', 'unknown')}
            
            indices_results = band_data | 'Calculate Indices' >> beam.Map(calculate_indices)
            
            def check_indices(result):
                self.assertGreater(len(result), 0)
                for indices in result:
                    if 'error' not in indices:
                        self.assertIn('ndvi', indices)
                        self.assertIn('gndvi', indices)
                        self.assertIn('tile_id', indices)
                        
                        # Check NDVI values
                        ndvi = indices['ndvi']
                        valid_ndvi = ndvi[~np.isnan(ndvi)]
                        if len(valid_ndvi) > 0:
                            self.assertTrue(np.all(valid_ndvi >= -1))
                            self.assertTrue(np.all(valid_ndvi <= 1))
            
            assert_that(indices_results, check_indices)
    
    def test_raster_mosaic_pipeline(self):
        """Test raster mosaicking pipeline."""
        with TestPipeline() as p:
            tiles = p | 'Create Tiles' >> beam.Create(self.sample_raster_data)
            
            # Create mosaic
            output_bounds = (-180, 38.8, -78.8, 90)  # Approximate bounds
            output_resolution = 0.1
            
            mosaic = tiles | 'Create Mosaic' >> RasterMosaicTransform(
                output_bounds=output_bounds,
                output_resolution=output_resolution
            )
            
            def check_mosaic(result):
                self.assertEqual(len(result), 1)
                mosaic_tile = result[0]
                self.assertIsInstance(mosaic_tile, RasterTile)
                self.assertEqual(mosaic_tile.tile_id, "mosaic")
                
                # Check dimensions
                expected_width = int((output_bounds[2] - output_bounds[0]) / output_resolution)
                expected_height = int((output_bounds[3] - output_bounds[1]) / output_resolution)
                
                self.assertEqual(mosaic_tile.data.shape[1], expected_height)
                self.assertEqual(mosaic_tile.data.shape[2], expected_width)
            
            assert_that(mosaic, check_mosaic)
    
    def test_temporal_analysis_simulation(self):
        """Test temporal analysis with simulated time series."""
        # Create temporal data (simulate 3 time points)
        temporal_data = []
        for t in range(3):
            for tile in self.sample_raster_data:
                # Add temporal variation
                temporal_tile_data = tile.data.copy().astype(np.float32)
                temporal_tile_data += np.random.normal(0, 50, temporal_tile_data.shape)
                
                temporal_tile = tile._replace(
                    data=temporal_tile_data,
                    tile_id=f"{tile.tile_id}_t{t}"
                )
                temporal_data.append(temporal_tile)
        
        with TestPipeline() as p:
            tiles = p | 'Create Temporal Tiles' >> beam.Create(temporal_data)
            
            # Group by spatial location
            def extract_spatial_key(tile):
                base_id = '_'.join(tile.tile_id.split('_')[:-1])  # Remove time suffix
                return (base_id, tile)
            
            spatial_groups = (
                tiles
                | 'Key by Location' >> beam.Map(extract_spatial_key)
                | 'Group by Location' >> beam.GroupByKey()
            )
            
            # Calculate temporal statistics
            def calculate_temporal_stats(location_tiles):
                location, tiles_list = location_tiles
                tiles_array = [tile.data for tile in tiles_list]
                
                if len(tiles_array) < 2:
                    return {'location': location, 'error': 'Insufficient data'}
                
                stacked = np.stack(tiles_array, axis=0)
                
                return {
                    'location': location,
                    'mean': np.nanmean(stacked, axis=0),
                    'std': np.nanstd(stacked, axis=0),
                    'range': np.nanmax(stacked, axis=0) - np.nanmin(stacked, axis=0)
                }
            
            temporal_stats = spatial_groups | 'Calculate Temporal Stats' >> beam.Map(
                calculate_temporal_stats
            )
            
            def check_temporal_stats(result):
                self.assertGreater(len(result), 0)
                for stats in result:
                    if 'error' not in stats:
                        self.assertIn('location', stats)
                        self.assertIn('mean', stats)
                        self.assertIn('std', stats)
                        self.assertIn('range', stats)
                        
                        # Check array shapes
                        self.assertEqual(stats['mean'].shape, (4, 512, 512))
                        self.assertEqual(stats['std'].shape, (4, 512, 512))
                        self.assertEqual(stats['range'].shape, (4, 512, 512))
            
            assert_that(temporal_stats, check_temporal_stats)
    
    def test_change_detection_pipeline(self):
        """Test change detection between two time periods."""
        # Create before and after datasets
        before_tiles = self.sample_raster_data[:2]  # First 2 tiles
        after_tiles = []
        
        for tile in before_tiles:
            # Simulate change by modifying values
            changed_data = tile.data.copy().astype(np.float32)
            changed_data[0] += 100  # Increase red band values
            changed_data[3] -= 50   # Decrease NIR values
            
            after_tile = tile._replace(
                data=changed_data,
                tile_id=f"{tile.tile_id}_after"
            )
            after_tiles.append(after_tile)
        
        with TestPipeline() as p:
            before = p | 'Create Before' >> beam.Create(before_tiles)
            after = p | 'Create After' >> beam.Create(after_tiles)
            
            # Key by spatial location
            def key_by_location(tile):
                base_id = tile.tile_id.replace('_after', '')
                return (base_id, tile)
            
            before_keyed = before | 'Key Before' >> beam.Map(key_by_location)
            after_keyed = after | 'Key After' >> beam.Map(key_by_location)
            
            # Combine before and after
            combined = (
                {'before': before_keyed, 'after': after_keyed}
                | 'Group by Location' >> beam.CoGroupByKey()
            )
            
            # Calculate change
            def calculate_change(location_data):
                location, data_dict = location_data
                before_tiles = list(data_dict['before'])
                after_tiles = list(data_dict['after'])
                
                if len(before_tiles) != 1 or len(after_tiles) != 1:
                    return {'location': location, 'error': 'Data mismatch'}
                
                before_data = before_tiles[0].data.astype(np.float32)
                after_data = after_tiles[0].data.astype(np.float32)
                
                # Calculate difference
                change = after_data - before_data
                
                # Calculate change magnitude
                change_magnitude = np.sqrt(np.sum(change ** 2, axis=0))
                
                return {
                    'location': location,
                    'change': change,
                    'magnitude': change_magnitude,
                    'max_change': np.max(change_magnitude)
                }
            
            change_results = combined | 'Calculate Change' >> beam.Map(calculate_change)
            
            def check_change_results(result):
                self.assertGreater(len(result), 0)
                for change_data in result:
                    if 'error' not in change_data:
                        self.assertIn('location', change_data)
                        self.assertIn('change', change_data)
                        self.assertIn('magnitude', change_data)
                        self.assertIn('max_change', change_data)
                        
                        # Should detect significant change
                        self.assertGreater(change_data['max_change'], 0)
            
            assert_that(change_results, check_change_results)
    
    def test_performance_simulation(self):
        """Test performance with larger dataset simulation."""
        # Create a larger dataset for performance testing
        large_dataset = []
        for i in range(10):  # 10 tiles
            data = np.random.randint(0, 1000, (4, 256, 256), dtype=np.uint16)
            tile = RasterTile(
                data=data,
                window=Window(0, 0, 256, 256),
                transform=(0.1, 0, -180, 0, -0.1, 90),
                bands=[1, 2, 3, 4],
                nodata=None,
                tile_id=f"tile_{i}"
            )
            large_dataset.append(tile)
        
        with TestPipeline() as p:
            tiles = p | 'Create Large Dataset' >> beam.Create(large_dataset)
            
            # Apply multiple transformations
            processed_tiles = (
                tiles
                | 'Calculate NDVI' >> RasterBandMathTransform(
                    expression="(B4 - B1) / (B4 + B1)",
                    output_name="ndvi"
                )
                | 'Filter Valid' >> RasterFilterTransform(
                    lambda data: np.any(~np.isnan(data))
                )
                | 'Add Metadata' >> beam.Map(
                    lambda tile: tile._replace(
                        tile_id=f"{tile.tile_id}_processed"
                    )
                )
            )
            
            def check_processed_tiles(result):
                self.assertGreater(len(result), 0)
                for tile in result:
                    self.assertIsInstance(tile, RasterTile)
                    self.assertTrue(tile.tile_id.endswith('_processed'))
                    # Should have 1 band (NDVI result)
                    self.assertEqual(tile.data.shape[0], 1)
            
            assert_that(processed_tiles, check_processed_tiles)
    
    def test_error_handling_pipeline(self):
        """Test pipeline error handling with invalid data."""
        # Create invalid data
        invalid_tiles = [
            RasterTile(
                data=np.full((4, 10, 10), np.nan, dtype=np.float32),  # All NaN
                window=Window(0, 0, 10, 10),
                transform=(0.1, 0, -180, 0, -0.1, 90),
                bands=[1, 2, 3, 4],
                nodata=np.nan,
                tile_id="invalid_tile"
            ),
            RasterTile(
                data=np.random.randint(0, 1000, (4, 10, 10), dtype=np.uint16),
                window=Window(0, 0, 10, 10),
                transform=(0.1, 0, -180, 0, -0.1, 90),
                bands=[1, 2, 3, 4],
                nodata=None,
                tile_id="valid_tile"
            )
        ]
        
        with TestPipeline() as p:
            tiles = p | 'Create Tiles' >> beam.Create(invalid_tiles)
            
            # Filter out invalid tiles
            valid_tiles = tiles | 'Filter Valid Tiles' >> RasterFilterTransform(
                lambda data: np.any(~np.isnan(data))
            )
            
            def check_valid_tiles(result):
                self.assertEqual(len(result), 1)
                self.assertEqual(result[0].tile_id, "valid_tile")
            
            assert_that(valid_tiles, check_valid_tiles)


class TestMemoryUsage(unittest.TestCase):
    """Test memory usage patterns for raster processing."""
    
    def test_streaming_processing(self):
        """Test that raster tiles are processed in streaming fashion."""
        def tile_generator():
            """Generator that yields tiles one by one."""
            for i in range(50):  # 50 tiles
                data = np.random.randint(0, 1000, (3, 128, 128), dtype=np.uint16)
                yield RasterTile(
                    data=data,
                    window=Window(0, 0, 128, 128),
                    transform=(0.1, 0, -180, 0, -0.1, 90),
                    bands=[1, 2, 3],
                    nodata=None,
                    tile_id=f"tile_{i}"
                )
        
        with TestPipeline() as p:
            tiles = p | 'Create Streaming Tiles' >> beam.Create(tile_generator())
            
            # Process tiles one by one
            processed = tiles | 'Process Streaming' >> beam.Map(
                lambda tile: tile._replace(
                    tile_id=f"{tile.tile_id}_processed"
                )
            )
            
            def check_streaming_processing(result):
                self.assertEqual(len(result), 50)
                for tile in result:
                    self.assertTrue(tile.tile_id.endswith('_processed'))
            
            assert_that(processed, check_streaming_processing)


if __name__ == '__main__':
    unittest.main()

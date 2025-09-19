"""Integration tests for FlatGeobuf reader with sample data."""

import json
import os
import tempfile
import unittest
from typing import List, Dict, Any

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

try:
    import shapely.geometry
    from shapely.geometry import Point, Polygon, mapping
except ImportError:
    shapely = None

from gexus.core.processor import GeoProcessor
from gexus.formats.flatgeobuf import FlatGeobufTransform
from gexus.transforms.spatial import (
    SpatialFilterTransform,
    GeometryValidationTransform,
    BoundsCalculationTransform
)


class TestFlatGeobufIntegration(unittest.TestCase):
    """Integration tests for FlatGeobuf reader with GeXus framework."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.sample_data = self._create_sample_geojson_data()
    
    def tearDown(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def _create_sample_geojson_data(self) -> List[Dict[str, Any]]:
        """Create sample GeoJSON-like data for testing."""
        return [
            {
                'type': 'Feature',
                'geometry': {
                    'type': 'Point',
                    'coordinates': [-122.4194, 37.7749]  # San Francisco
                },
                'properties': {
                    'name': 'San Francisco',
                    'population': 883305,
                    'state': 'California'
                }
            },
            {
                'type': 'Feature',
                'geometry': {
                    'type': 'Point',
                    'coordinates': [-74.0060, 40.7128]  # New York
                },
                'properties': {
                    'name': 'New York',
                    'population': 8336817,
                    'state': 'New York'
                }
            },
            {
                'type': 'Feature',
                'geometry': {
                    'type': 'Point',
                    'coordinates': [-87.6298, 41.8781]  # Chicago
                },
                'properties': {
                    'name': 'Chicago',
                    'population': 2693976,
                    'state': 'Illinois'
                }
            },
            {
                'type': 'Feature',
                'geometry': {
                    'type': 'Polygon',
                    'coordinates': [[
                        [-122.5, 37.7], [-122.3, 37.7],
                        [-122.3, 37.8], [-122.5, 37.8],
                        [-122.5, 37.7]
                    ]]
                },
                'properties': {
                    'name': 'Bay Area',
                    'type': 'region',
                    'area_km2': 18040
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
    
    def test_feature_processing_pipeline(self):
        """Test complete feature processing pipeline."""
        with TestPipeline() as p:
            # Create features from sample data
            features = p | 'Create Features' >> beam.Create(self.sample_data)
            
            # Apply spatial filter (West Coast)
            west_coast_bbox = (-125.0, 32.0, -114.0, 42.0)
            filtered_features = features | 'Filter West Coast' >> SpatialFilterTransform(
                bbox=west_coast_bbox
            )
            
            # Validate geometries
            validated_features = filtered_features | 'Validate Geometries' >> GeometryValidationTransform()
            
            # Calculate bounds
            feature_bounds = validated_features | 'Calculate Bounds' >> BoundsCalculationTransform()
            
            # Extract just the features for testing
            final_features = feature_bounds | 'Extract Features' >> beam.Map(lambda x: x[0])
            
            # Verify results
            def check_west_coast_features(result):
                self.assertGreater(len(result), 0)
                # Should contain San Francisco but not New York or Chicago
                names = [f['properties']['name'] for f in result]
                self.assertIn('San Francisco', names)
                self.assertNotIn('New York', names)
                self.assertNotIn('Chicago', names)
            
            assert_that(final_features, check_west_coast_features)
    
    def test_property_filtering(self):
        """Test filtering features by properties."""
        with TestPipeline() as p:
            features = p | 'Create Features' >> beam.Create(self.sample_data)
            
            # Filter by population > 2 million
            large_cities = features | 'Filter Large Cities' >> beam.Filter(
                lambda f: f['properties'].get('population', 0) > 2000000
            )
            
            def check_large_cities(result):
                self.assertEqual(len(result), 2)  # New York and Chicago
                names = [f['properties']['name'] for f in result]
                self.assertIn('New York', names)
                self.assertIn('Chicago', names)
                self.assertNotIn('San Francisco', names)
            
            assert_that(large_cities, check_large_cities)
    
    def test_geometry_type_filtering(self):
        """Test filtering by geometry type."""
        with TestPipeline() as p:
            features = p | 'Create Features' >> beam.Create(self.sample_data)
            
            # Filter only points
            points_only = features | 'Filter Points' >> beam.Filter(
                lambda f: f['geometry']['type'] == 'Point'
            )
            
            # Filter only polygons
            polygons_only = features | 'Filter Polygons' >> beam.Filter(
                lambda f: f['geometry']['type'] == 'Polygon'
            )
            
            def check_points(result):
                self.assertEqual(len(result), 3)  # Three cities
                for feature in result:
                    self.assertEqual(feature['geometry']['type'], 'Point')
            
            def check_polygons(result):
                self.assertEqual(len(result), 1)  # Bay Area region
                for feature in result:
                    self.assertEqual(feature['geometry']['type'], 'Polygon')
            
            assert_that(points_only, check_points)
            assert_that(polygons_only, check_polygons)
    
    @unittest.skipIf(shapely is None, "Shapely not available")
    def test_spatial_operations(self):
        """Test spatial operations on features."""
        with TestPipeline() as p:
            features = p | 'Create Features' >> beam.Create(self.sample_data)
            
            # Calculate centroids for polygons
            def calculate_centroid(feature):
                if feature['geometry']['type'] == 'Polygon':
                    geom = shapely.geometry.shape(feature['geometry'])
                    centroid = geom.centroid
                    feature['properties']['centroid'] = list(centroid.coords[0])
                return feature
            
            features_with_centroids = features | 'Calculate Centroids' >> beam.Map(calculate_centroid)
            
            def check_centroids(result):
                for feature in result:
                    if feature['geometry']['type'] == 'Polygon':
                        self.assertIn('centroid', feature['properties'])
                        centroid = feature['properties']['centroid']
                        self.assertEqual(len(centroid), 2)  # x, y coordinates
            
            assert_that(features_with_centroids, check_centroids)
    
    def test_aggregation_operations(self):
        """Test aggregation operations on features."""
        with TestPipeline() as p:
            features = p | 'Create Features' >> beam.Create(self.sample_data)
            
            # Extract populations and calculate total
            populations = features | 'Extract Populations' >> beam.Map(
                lambda f: f['properties'].get('population', 0)
            )
            
            total_population = populations | 'Sum Population' >> beam.CombineGlobally(sum)
            
            def check_total_population(result):
                self.assertEqual(len(result), 1)
                # San Francisco + New York + Chicago = 883305 + 8336817 + 2693976
                expected_total = 883305 + 8336817 + 2693976
                self.assertEqual(result[0], expected_total)
            
            assert_that(total_population, check_total_population)
    
    def test_grouping_operations(self):
        """Test grouping operations on features."""
        with TestPipeline() as p:
            features = p | 'Create Features' >> beam.Create(self.sample_data)
            
            # Group by state
            def extract_state_population(feature):
                state = feature['properties'].get('state')
                population = feature['properties'].get('population', 0)
                if state and population > 0:
                    return (state, population)
                return None
            
            state_populations = (
                features
                | 'Extract State Population' >> beam.Map(extract_state_population)
                | 'Filter None' >> beam.Filter(lambda x: x is not None)
                | 'Group by State' >> beam.GroupByKey()
                | 'Sum by State' >> beam.Map(lambda x: (x[0], sum(x[1])))
            )
            
            def check_state_populations(result):
                state_dict = dict(result)
                self.assertIn('California', state_dict)
                self.assertIn('New York', state_dict)
                self.assertIn('Illinois', state_dict)
                
                self.assertEqual(state_dict['California'], 883305)
                self.assertEqual(state_dict['New York'], 8336817)
                self.assertEqual(state_dict['Illinois'], 2693976)
            
            assert_that(state_populations, check_state_populations)
    
    def test_error_handling_pipeline(self):
        """Test pipeline error handling with invalid data."""
        invalid_data = [
            {
                'type': 'Feature',
                'geometry': None,  # Invalid geometry
                'properties': {'name': 'Invalid Feature'}
            },
            {
                'type': 'Feature',
                'geometry': {
                    'type': 'Point',
                    'coordinates': [0, 0]
                },
                'properties': {'name': 'Valid Feature'}
            }
        ]
        
        with TestPipeline() as p:
            features = p | 'Create Features' >> beam.Create(invalid_data)
            
            # Filter out features with invalid geometries
            valid_features = features | 'Filter Valid' >> beam.Filter(
                lambda f: f['geometry'] is not None
            )
            
            def check_valid_features(result):
                self.assertEqual(len(result), 1)
                self.assertEqual(result[0]['properties']['name'], 'Valid Feature')
            
            assert_that(valid_features, check_valid_features)
    
    def test_performance_simulation(self):
        """Test performance with larger dataset simulation."""
        # Create a larger dataset for performance testing
        large_dataset = []
        for i in range(1000):
            feature = {
                'type': 'Feature',
                'geometry': {
                    'type': 'Point',
                    'coordinates': [-180 + (i % 360), -90 + (i % 180)]
                },
                'properties': {
                    'id': i,
                    'name': f'Feature_{i}',
                    'value': i * 1.5
                }
            }
            large_dataset.append(feature)
        
        with TestPipeline() as p:
            features = p | 'Create Large Dataset' >> beam.Create(large_dataset)
            
            # Apply multiple transformations
            processed_features = (
                features
                | 'Validate Geometries' >> GeometryValidationTransform()
                | 'Filter by Value' >> beam.Filter(lambda f: f['properties']['value'] > 500)
                | 'Add Computed Property' >> beam.Map(
                    lambda f: {**f, 'properties': {**f['properties'], 'computed': f['properties']['value'] * 2}}
                )
            )
            
            def check_processed_features(result):
                self.assertGreater(len(result), 0)
                for feature in result:
                    self.assertGreater(feature['properties']['value'], 500)
                    self.assertIn('computed', feature['properties'])
                    expected_computed = feature['properties']['value'] * 2
                    self.assertEqual(feature['properties']['computed'], expected_computed)
            
            assert_that(processed_features, check_processed_features)


class TestMemoryUsage(unittest.TestCase):
    """Test memory usage patterns."""
    
    def test_streaming_processing(self):
        """Test that features are processed in streaming fashion."""
        # This test verifies that we don't load all features into memory at once
        def feature_generator():
            """Generator that yields features one by one."""
            for i in range(100):
                yield {
                    'type': 'Feature',
                    'geometry': {
                        'type': 'Point',
                        'coordinates': [i, i]
                    },
                    'properties': {'id': i}
                }
        
        with TestPipeline() as p:
            features = p | 'Create Streaming Features' >> beam.Create(feature_generator())
            
            # Process features one by one
            processed = features | 'Process Streaming' >> beam.Map(
                lambda f: {**f, 'properties': {**f['properties'], 'processed': True}}
            )
            
            def check_streaming_processing(result):
                self.assertEqual(len(result), 100)
                for feature in result:
                    self.assertTrue(feature['properties']['processed'])
            
            assert_that(processed, check_streaming_processing)


if __name__ == '__main__':
    unittest.main()

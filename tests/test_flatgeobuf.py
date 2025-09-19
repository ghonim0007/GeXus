"""Unit tests for FlatGeobuf reader implementation."""

import json
import os
import tempfile
import unittest
from unittest.mock import Mock, patch, MagicMock
from typing import Dict, Any

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

try:
    import shapely.geometry
    from shapely.geometry import Point, Polygon, mapping
except ImportError:
    shapely = None

from gexus.formats.flatgeobuf import (
    FlatGeobufReader,
    FlatGeobufTransform,
    ReadFlatGeobufFn,
    SplittableFlatGeobufFn
)
from gexus.transforms.spatial import (
    SpatialFilterTransform,
    GeometryValidationTransform,
    CRSTransformTransform
)


class TestFlatGeobufReader(unittest.TestCase):
    """Test cases for FlatGeobufReader class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.test_file_path = "/tmp/test.fgb"
        
        # Mock FlatGeobuf reader components
        self.mock_reader = Mock()
        self.mock_header = Mock()
        self.mock_envelope = Mock()
        
        # Configure mock envelope
        self.mock_envelope.min_x.return_value = -180.0
        self.mock_envelope.min_y.return_value = -90.0
        self.mock_envelope.max_x.return_value = 180.0
        self.mock_envelope.max_y.return_value = 90.0
        
        # Configure mock header
        self.mock_header.envelope.return_value = self.mock_envelope
        self.mock_header.features_count.return_value = 100
        self.mock_header.geometry_type.return_value = 1  # Point
        self.mock_header.columns_length.return_value = 2
        self.mock_header.crs.return_value = None
        
        # Mock columns
        mock_column1 = Mock()
        mock_column1.name.return_value = b'name'
        mock_column1.type.return_value = 11  # string
        
        mock_column2 = Mock()
        mock_column2.name.return_value = b'value'
        mock_column2.type.return_value = 9  # float
        
        self.mock_header.columns.side_effect = lambda i: [mock_column1, mock_column2][i]
        
        # Configure mock reader
        self.mock_reader.header.return_value = self.mock_header
    
    @patch('gexus.formats.flatgeobuf.os.path.exists')
    @patch('builtins.open')
    @patch('gexus.formats.flatgeobuf.Reader')
    def test_initialization(self, mock_reader_class, mock_open, mock_exists):
        """Test FlatGeobufReader initialization."""
        mock_exists.return_value = True
        mock_reader_class.return_value = self.mock_reader
        
        reader = FlatGeobufReader(self.test_file_path)
        
        self.assertEqual(reader.file_path, self.test_file_path)
        self.assertIsNotNone(reader._schema)
        self.assertEqual(reader._bounds, (-180.0, -90.0, 180.0, 90.0))
        self.assertEqual(reader._feature_count, 100)
    
    @patch('gexus.formats.flatgeobuf.os.path.exists')
    def test_file_not_found(self, mock_exists):
        """Test handling of missing files."""
        mock_exists.return_value = False
        
        with self.assertRaises(FileNotFoundError):
            FlatGeobufReader(self.test_file_path)
    
    @patch('gexus.formats.flatgeobuf.os.path.exists')
    @patch('builtins.open')
    @patch('gexus.formats.flatgeobuf.Reader')
    def test_get_schema(self, mock_reader_class, mock_open, mock_exists):
        """Test schema extraction."""
        mock_exists.return_value = True
        mock_reader_class.return_value = self.mock_reader
        
        reader = FlatGeobufReader(self.test_file_path)
        schema = reader.get_schema()
        
        expected_schema = {
            'geometry_type': 'Point',
            'properties': {
                'name': 'string',
                'value': 'float'
            },
            'crs': None
        }
        
        self.assertEqual(schema, expected_schema)
    
    @patch('gexus.formats.flatgeobuf.os.path.exists')
    @patch('builtins.open')
    @patch('gexus.formats.flatgeobuf.Reader')
    def test_get_bounds(self, mock_reader_class, mock_open, mock_exists):
        """Test bounds extraction."""
        mock_exists.return_value = True
        mock_reader_class.return_value = self.mock_reader
        
        reader = FlatGeobufReader(self.test_file_path)
        bounds = reader.get_bounds()
        
        self.assertEqual(bounds, (-180.0, -90.0, 180.0, 90.0))
    
    @patch('gexus.formats.flatgeobuf.os.path.exists')
    @patch('builtins.open')
    @patch('gexus.formats.flatgeobuf.Reader')
    def test_get_feature_count(self, mock_reader_class, mock_open, mock_exists):
        """Test feature count extraction."""
        mock_exists.return_value = True
        mock_reader_class.return_value = self.mock_reader
        
        reader = FlatGeobufReader(self.test_file_path)
        count = reader.get_feature_count()
        
        self.assertEqual(count, 100)
    
    @patch('gexus.formats.flatgeobuf.os.path.exists')
    @patch('builtins.open')
    @patch('gexus.formats.flatgeobuf.Reader')
    def test_read_features(self, mock_reader_class, mock_open, mock_exists):
        """Test feature reading."""
        mock_exists.return_value = True
        
        # Create mock features
        mock_feature = Mock()
        mock_feature.geometry_geojson.return_value = {
            'type': 'Point',
            'coordinates': [0.0, 0.0]
        }
        
        mock_properties = Mock()
        mock_properties.name = 'test_point'
        mock_properties.value = 42.5
        mock_feature.properties.return_value = mock_properties
        
        # Configure reader to return mock features
        mock_reader_instance = Mock()
        mock_reader_instance.header.return_value = self.mock_header
        mock_reader_instance.__iter__ = Mock(return_value=iter([mock_feature]))
        
        mock_reader_class.return_value = mock_reader_instance
        
        reader = FlatGeobufReader(self.test_file_path)
        
        # Mock the second reader instance for feature iteration
        with patch('builtins.open'), patch('gexus.formats.flatgeobuf.Reader') as mock_reader2:
            mock_reader2.return_value = mock_reader_instance
            
            features = list(reader.read_features())
            
            self.assertEqual(len(features), 1)
            feature = features[0]
            
            self.assertEqual(feature['type'], 'Feature')
            self.assertEqual(feature['geometry']['type'], 'Point')
            self.assertEqual(feature['geometry']['coordinates'], [0.0, 0.0])
    
    def test_geometry_type_mapping(self):
        """Test geometry type mapping."""
        with patch('gexus.formats.flatgeobuf.os.path.exists'), \
             patch('builtins.open'), \
             patch('gexus.formats.flatgeobuf.Reader'):
            
            reader = FlatGeobufReader(self.test_file_path)
            
            # Test various geometry types
            self.assertEqual(reader._get_geometry_type(1), 'Point')
            self.assertEqual(reader._get_geometry_type(2), 'LineString')
            self.assertEqual(reader._get_geometry_type(3), 'Polygon')
            self.assertEqual(reader._get_geometry_type(4), 'MultiPoint')
            self.assertEqual(reader._get_geometry_type(999), 'Unknown')
    
    def test_property_type_mapping(self):
        """Test property type mapping."""
        with patch('gexus.formats.flatgeobuf.os.path.exists'), \
             patch('builtins.open'), \
             patch('gexus.formats.flatgeobuf.Reader'):
            
            reader = FlatGeobufReader(self.test_file_path)
            
            # Test various property types
            self.assertEqual(reader._get_property_type(2), 'bool')
            self.assertEqual(reader._get_property_type(5), 'int')
            self.assertEqual(reader._get_property_type(9), 'float')
            self.assertEqual(reader._get_property_type(11), 'string')
            self.assertEqual(reader._get_property_type(999), 'unknown')


class TestReadFlatGeobufFn(unittest.TestCase):
    """Test cases for ReadFlatGeobufFn DoFn."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.test_file_path = "/tmp/test.fgb"
    
    @patch('gexus.formats.flatgeobuf.FlatGeobufReader')
    def test_process(self, mock_reader_class):
        """Test DoFn processing."""
        # Mock reader and features
        mock_reader = Mock()
        mock_features = [
            {
                'type': 'Feature',
                'geometry': {'type': 'Point', 'coordinates': [0.0, 0.0]},
                'properties': {'name': 'test1'}
            },
            {
                'type': 'Feature',
                'geometry': {'type': 'Point', 'coordinates': [1.0, 1.0]},
                'properties': {'name': 'test2'}
            }
        ]
        mock_reader.read_features.return_value = iter(mock_features)
        mock_reader_class.return_value = mock_reader
        
        # Test DoFn
        dofn = ReadFlatGeobufFn()
        results = list(dofn.process(self.test_file_path))
        
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]['properties']['name'], 'test1')
        self.assertEqual(results[1]['properties']['name'], 'test2')
    
    @patch('gexus.formats.flatgeobuf.FlatGeobufReader')
    def test_process_error_handling(self, mock_reader_class):
        """Test DoFn error handling."""
        mock_reader_class.side_effect = Exception("Test error")
        
        dofn = ReadFlatGeobufFn()
        
        with self.assertRaises(Exception):
            list(dofn.process(self.test_file_path))


class TestSplittableFlatGeobufFn(unittest.TestCase):
    """Test cases for SplittableFlatGeobufFn DoFn."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.test_file_path = "/tmp/test.fgb"
    
    @patch('gexus.formats.flatgeobuf.FlatGeobufReader')
    def test_create_initial_restriction(self, mock_reader_class):
        """Test initial restriction creation."""
        mock_reader = Mock()
        mock_reader.get_feature_count.return_value = 1000
        mock_reader_class.return_value = mock_reader
        
        dofn = SplittableFlatGeobufFn()
        restriction = dofn.create_initial_restriction(self.test_file_path)
        
        self.assertEqual(restriction.start, 0)
        self.assertEqual(restriction.stop, 1000)
    
    @patch('gexus.formats.flatgeobuf.FlatGeobufReader')
    def test_split_restriction(self, mock_reader_class):
        """Test restriction splitting."""
        from apache_beam.io.restriction_trackers import OffsetRange
        
        dofn = SplittableFlatGeobufFn()
        restriction = OffsetRange(0, 3000)
        splits = dofn.split(self.test_file_path, restriction)
        
        # Should split into chunks of 1000
        self.assertEqual(len(splits), 3)
        self.assertEqual(splits[0], OffsetRange(0, 1000))
        self.assertEqual(splits[1], OffsetRange(1000, 2000))
        self.assertEqual(splits[2], OffsetRange(2000, 3000))
    
    def test_restriction_size(self):
        """Test restriction size calculation."""
        from apache_beam.io.restriction_trackers import OffsetRange
        
        dofn = SplittableFlatGeobufFn()
        restriction = OffsetRange(100, 500)
        size = dofn.restriction_size(self.test_file_path, restriction)
        
        self.assertEqual(size, 400)


class TestFlatGeobufTransform(unittest.TestCase):
    """Test cases for FlatGeobufTransform."""
    
    def test_initialization(self):
        """Test transform initialization."""
        transform = FlatGeobufTransform("*.fgb", use_splittable=True)
        
        self.assertEqual(transform.file_pattern, "*.fgb")
        self.assertTrue(transform.use_splittable)
    
    def test_initialization_with_options(self):
        """Test transform initialization with options."""
        transform = FlatGeobufTransform(
            "*.fgb",
            use_splittable=False,
            bbox=(-180, -90, 180, 90),
            validate_geometry=True
        )
        
        self.assertEqual(transform.file_pattern, "*.fgb")
        self.assertFalse(transform.use_splittable)
        self.assertEqual(transform.options['bbox'], (-180, -90, 180, 90))
        self.assertTrue(transform.options['validate_geometry'])


@unittest.skipIf(shapely is None, "Shapely not available")
class TestSpatialTransforms(unittest.TestCase):
    """Test cases for spatial transforms."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.test_features = [
            {
                'type': 'Feature',
                'geometry': mapping(Point(0, 0)),
                'properties': {'name': 'center'}
            },
            {
                'type': 'Feature',
                'geometry': mapping(Point(10, 10)),
                'properties': {'name': 'corner'}
            }
        ]
    
    def test_spatial_filter_transform(self):
        """Test spatial filter transform."""
        with TestPipeline() as p:
            # Create test data
            features = p | beam.Create(self.test_features)
            
            # Apply spatial filter (bbox that includes only the first point)
            filtered = features | SpatialFilterTransform(bbox=(-1, -1, 1, 1))
            
            # The result should contain only the center point
            def check_result(result):
                self.assertEqual(len(result), 1)
                self.assertEqual(result[0]['properties']['name'], 'center')
            
            assert_that(filtered, check_result)
    
    def test_geometry_validation_transform(self):
        """Test geometry validation transform."""
        with TestPipeline() as p:
            # Create test data with valid geometries
            features = p | beam.Create(self.test_features)
            
            # Apply geometry validation
            validated = features | GeometryValidationTransform(fix_invalid=True)
            
            # Should return all features since they're valid
            assert_that(validated, equal_to(self.test_features))


class TestIntegration(unittest.TestCase):
    """Integration tests for FlatGeobuf reader."""
    
    @patch('gexus.formats.flatgeobuf.FlatGeobufReader')
    def test_end_to_end_pipeline(self, mock_reader_class):
        """Test end-to-end pipeline with FlatGeobuf reader."""
        # Mock reader
        mock_reader = Mock()
        mock_features = [
            {
                'type': 'Feature',
                'geometry': {'type': 'Point', 'coordinates': [0.0, 0.0]},
                'properties': {'name': 'test1', 'value': 10}
            },
            {
                'type': 'Feature',
                'geometry': {'type': 'Point', 'coordinates': [1.0, 1.0]},
                'properties': {'name': 'test2', 'value': 20}
            }
        ]
        mock_reader.read_features.return_value = iter(mock_features)
        mock_reader_class.return_value = mock_reader
        
        # Create pipeline
        with TestPipeline() as p:
            # Mock file matching
            with patch('apache_beam.io.fileio.MatchFiles') as mock_match, \
                 patch('apache_beam.io.fileio.ReadMatches') as mock_read:
                
                mock_file_metadata = Mock()
                mock_file_metadata.path = '/tmp/test.fgb'
                mock_file = Mock()
                mock_file.metadata = mock_file_metadata
                
                mock_match.return_value = beam.Create([mock_file])
                mock_read.return_value = beam.Create([mock_file])
                
                # Read features
                features = (
                    p
                    | 'Start' >> beam.Create([None])
                    | 'Read FlatGeobuf' >> FlatGeobufTransform('*.fgb', use_splittable=False)
                )
                
                # Verify results
                def check_features(result):
                    self.assertEqual(len(result), 2)
                    names = [f['properties']['name'] for f in result]
                    self.assertIn('test1', names)
                    self.assertIn('test2', names)
                
                assert_that(features, check_features)


if __name__ == '__main__':
    unittest.main()

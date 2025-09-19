"""Unit tests for GeoTIFF reader implementation."""

import json
import os
import tempfile
import unittest
from unittest.mock import Mock, patch, MagicMock
from typing import Dict, Any
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

from gexus.formats.geotiff import (
    GeoTIFFReader,
    GeoTIFFTransform,
    ReadGeoTIFFTilesFn,
    ReadGeoTIFFBandsFn,
    SplittableGeoTIFFReaderFn
)
from gexus.formats.raster_base import RasterTile, RasterBand
from gexus.transforms.raster import (
    RasterBandMathTransform,
    RasterResampleTransform,
    ZonalStatisticsTransform
)


class TestGeoTIFFReader(unittest.TestCase):
    """Test cases for GeoTIFFReader class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.test_file_path = "/tmp/test.tif"
        
        # Mock rasterio dataset
        self.mock_src = Mock()
        self.mock_src.width = 1000
        self.mock_src.height = 1000
        self.mock_src.count = 3
        self.mock_src.dtype = 'uint16'
        self.mock_src.crs = CRS.from_epsg(4326)
        self.mock_src.transform = Affine(0.1, 0, -180, 0, -0.1, 90)
        self.mock_src.nodata = None
        self.mock_src.bounds = rasterio.coords.BoundingBox(-180, -90, 180, 90) if rasterio else None
        self.mock_src.res = (0.1, 0.1)
        self.mock_src.compression = None
        self.mock_src.interleaving = None
        self.mock_src.is_tiled = True
        self.mock_src.block_shapes = [(512, 512)]
        self.mock_src.descriptions = ['Red', 'Green', 'Blue']
        self.mock_src.colorinterp = [Mock(name='red'), Mock(name='green'), Mock(name='blue')]
        self.mock_src.units = [None, None, None]
        self.mock_src.offsets = [None, None, None]
        self.mock_src.scales = [None, None, None]
        
        # Configure colorinterp mock
        for i, interp in enumerate(self.mock_src.colorinterp):
            interp.name = ['red', 'green', 'blue'][i]
        
        # Mock overviews
        self.mock_src.overviews.return_value = [2, 4, 8]
    
    @patch('gexus.formats.geotiff.os.path.exists')
    @patch('gexus.formats.geotiff.rasterio.open')
    def test_initialization(self, mock_open, mock_exists):
        """Test GeoTIFFReader initialization."""
        mock_exists.return_value = True
        mock_open.return_value.__enter__.return_value = self.mock_src
        
        reader = GeoTIFFReader(self.test_file_path)
        
        self.assertEqual(reader.file_path, self.test_file_path)
        self.assertIsNotNone(reader._metadata)
        self.assertEqual(reader._metadata.width, 1000)
        self.assertEqual(reader._metadata.height, 1000)
        self.assertEqual(reader._metadata.count, 3)
    
    @patch('gexus.formats.geotiff.os.path.exists')
    def test_file_not_found(self, mock_exists):
        """Test handling of missing files."""
        mock_exists.return_value = False
        
        with self.assertRaises(FileNotFoundError):
            GeoTIFFReader(self.test_file_path)
    
    @patch('gexus.formats.geotiff.os.path.exists')
    @patch('gexus.formats.geotiff.rasterio.open')
    def test_get_metadata(self, mock_open, mock_exists):
        """Test metadata extraction."""
        mock_exists.return_value = True
        mock_open.return_value.__enter__.return_value = self.mock_src
        
        reader = GeoTIFFReader(self.test_file_path)
        metadata = reader.get_metadata()
        
        self.assertEqual(metadata['width'], 1000)
        self.assertEqual(metadata['height'], 1000)
        self.assertEqual(metadata['count'], 3)
        self.assertEqual(metadata['dtype'], 'uint16')
    
    @patch('gexus.formats.geotiff.os.path.exists')
    @patch('gexus.formats.geotiff.rasterio.open')
    def test_get_shape(self, mock_open, mock_exists):
        """Test shape extraction."""
        mock_exists.return_value = True
        mock_open.return_value.__enter__.return_value = self.mock_src
        
        reader = GeoTIFFReader(self.test_file_path)
        shape = reader.get_shape()
        
        self.assertEqual(shape, (3, 1000, 1000))
    
    @patch('gexus.formats.geotiff.os.path.exists')
    @patch('gexus.formats.geotiff.rasterio.open')
    def test_get_overviews(self, mock_open, mock_exists):
        """Test overview extraction."""
        mock_exists.return_value = True
        mock_open.return_value.__enter__.return_value = self.mock_src
        
        reader = GeoTIFFReader(self.test_file_path)
        overviews = reader.get_overviews()
        
        self.assertEqual(overviews, [2, 4, 8])
    
    @patch('gexus.formats.geotiff.os.path.exists')
    @patch('gexus.formats.geotiff.rasterio.open')
    def test_read_bands(self, mock_open, mock_exists):
        """Test band reading."""
        mock_exists.return_value = True
        
        # Create mock band data
        mock_band_data = np.random.randint(0, 1000, (1000, 1000), dtype=np.uint16)
        self.mock_src.read.return_value = mock_band_data
        
        mock_open.return_value.__enter__.return_value = self.mock_src
        
        reader = GeoTIFFReader(self.test_file_path)
        bands = list(reader.read_bands([1]))
        
        self.assertEqual(len(bands), 1)
        band = bands[0]
        self.assertIsInstance(band, RasterBand)
        self.assertEqual(band.band_number, 1)
        self.assertEqual(band.data.shape, (1000, 1000))
    
    @patch('gexus.formats.geotiff.os.path.exists')
    @patch('gexus.formats.geotiff.rasterio.open')
    def test_read_tiles(self, mock_open, mock_exists):
        """Test tile reading."""
        mock_exists.return_value = True
        
        # Create mock tile data
        mock_tile_data = np.random.randint(0, 1000, (3, 512, 512), dtype=np.uint16)
        self.mock_src.read.return_value = mock_tile_data
        
        mock_open.return_value.__enter__.return_value = self.mock_src
        
        reader = GeoTIFFReader(self.test_file_path)
        tiles = list(reader.read_tiles(tile_size=(512, 512)))
        
        # Should have 4 tiles (2x2 grid for 1000x1000 image with 512x512 tiles)
        self.assertGreater(len(tiles), 0)
        
        tile = tiles[0]
        self.assertIsInstance(tile, RasterTile)
        self.assertEqual(tile.data.shape[0], 3)  # 3 bands
        self.assertLessEqual(tile.data.shape[1], 512)  # Height <= tile size
        self.assertLessEqual(tile.data.shape[2], 512)  # Width <= tile size
    
    @patch('gexus.formats.geotiff.os.path.exists')
    @patch('gexus.formats.geotiff.rasterio.open')
    def test_read_window(self, mock_open, mock_exists):
        """Test window reading."""
        mock_exists.return_value = True
        
        # Create mock window data
        mock_window_data = np.random.randint(0, 1000, (3, 100, 100), dtype=np.uint16)
        self.mock_src.read.return_value = mock_window_data
        
        mock_open.return_value.__enter__.return_value = self.mock_src
        
        reader = GeoTIFFReader(self.test_file_path)
        window = Window(0, 0, 100, 100)
        data = reader.read_window(window, bands=[1, 2, 3])
        
        self.assertEqual(data.shape, (3, 100, 100))
    
    @patch('gexus.formats.geotiff.os.path.exists')
    @patch('gexus.formats.geotiff.rasterio.open')
    def test_spatial_filtering(self, mock_open, mock_exists):
        """Test spatial filtering with bounding box."""
        mock_exists.return_value = True
        mock_open.return_value.__enter__.return_value = self.mock_src
        
        bbox = (-90, -45, 90, 45)
        reader = GeoTIFFReader(self.test_file_path, bbox=bbox)
        
        self.assertEqual(reader.bbox, bbox)


class TestReadGeoTIFFTilesFn(unittest.TestCase):
    """Test cases for ReadGeoTIFFTilesFn DoFn."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.test_file_path = "/tmp/test.tif"
    
    @patch('gexus.formats.geotiff.GeoTIFFReader')
    def test_process(self, mock_reader_class):
        """Test DoFn processing."""
        # Mock reader and tiles
        mock_reader = Mock()
        mock_tiles = [
            RasterTile(
                data=np.random.randint(0, 1000, (3, 512, 512), dtype=np.uint16),
                window=Window(0, 0, 512, 512),
                transform=(0.1, 0, -180, 0, -0.1, 90),
                bands=[1, 2, 3],
                nodata=None,
                tile_id="0_0"
            ),
            RasterTile(
                data=np.random.randint(0, 1000, (3, 512, 512), dtype=np.uint16),
                window=Window(512, 0, 512, 512),
                transform=(0.1, 0, -180, 0, -0.1, 90),
                bands=[1, 2, 3],
                nodata=None,
                tile_id="0_1"
            )
        ]
        mock_reader.read_tiles.return_value = iter(mock_tiles)
        mock_reader_class.return_value = mock_reader
        
        # Test DoFn
        dofn = ReadGeoTIFFTilesFn(tile_size=(512, 512))
        results = list(dofn.process(self.test_file_path))
        
        self.assertEqual(len(results), 2)
        self.assertIsInstance(results[0], RasterTile)
        self.assertEqual(results[0].tile_id, "0_0")
        self.assertEqual(results[1].tile_id, "0_1")
    
    @patch('gexus.formats.geotiff.GeoTIFFReader')
    def test_process_error_handling(self, mock_reader_class):
        """Test DoFn error handling."""
        mock_reader_class.side_effect = Exception("Test error")
        
        dofn = ReadGeoTIFFTilesFn()
        
        with self.assertRaises(Exception):
            list(dofn.process(self.test_file_path))


class TestReadGeoTIFFBandsFn(unittest.TestCase):
    """Test cases for ReadGeoTIFFBandsFn DoFn."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.test_file_path = "/tmp/test.tif"
    
    @patch('gexus.formats.geotiff.GeoTIFFReader')
    def test_process(self, mock_reader_class):
        """Test DoFn processing."""
        # Mock reader and bands
        mock_reader = Mock()
        mock_bands = [
            RasterBand(
                data=np.random.randint(0, 1000, (1000, 1000), dtype=np.uint16),
                band_number=1,
                nodata=None,
                dtype=np.uint16,
                description='Red',
                color_interp='red'
            ),
            RasterBand(
                data=np.random.randint(0, 1000, (1000, 1000), dtype=np.uint16),
                band_number=2,
                nodata=None,
                dtype=np.uint16,
                description='Green',
                color_interp='green'
            )
        ]
        mock_reader.read_bands.return_value = iter(mock_bands)
        mock_reader_class.return_value = mock_reader
        
        # Test DoFn
        dofn = ReadGeoTIFFBandsFn()
        results = list(dofn.process(self.test_file_path))
        
        self.assertEqual(len(results), 2)
        self.assertIsInstance(results[0], RasterBand)
        self.assertEqual(results[0].band_number, 1)
        self.assertEqual(results[1].band_number, 2)


class TestSplittableGeoTIFFReaderFn(unittest.TestCase):
    """Test cases for SplittableGeoTIFFReaderFn DoFn."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.test_file_path = "/tmp/test.tif"
    
    @patch('gexus.formats.geotiff.GeoTIFFReader')
    def test_create_initial_restriction(self, mock_reader_class):
        """Test initial restriction creation."""
        mock_reader = Mock()
        mock_reader.get_shape.return_value = (3, 1000, 1000)  # 3 bands, 1000x1000
        mock_reader_class.return_value = mock_reader
        
        dofn = SplittableGeoTIFFReaderFn(tile_size=(512, 512))
        restriction = dofn.create_initial_restriction(self.test_file_path)
        
        # Should have 4 tiles (2x2 grid)
        self.assertEqual(restriction.start, 0)
        self.assertEqual(restriction.stop, 4)
    
    @patch('gexus.formats.geotiff.GeoTIFFReader')
    def test_split_restriction(self, mock_reader_class):
        """Test restriction splitting."""
        from apache_beam.io.restriction_trackers import OffsetRange
        
        dofn = SplittableGeoTIFFReaderFn()
        restriction = OffsetRange(0, 300)  # 300 tiles
        splits = dofn.split(self.test_file_path, restriction)
        
        # Should split into chunks of 100
        self.assertEqual(len(splits), 3)
        self.assertEqual(splits[0], OffsetRange(0, 100))
        self.assertEqual(splits[1], OffsetRange(100, 200))
        self.assertEqual(splits[2], OffsetRange(200, 300))
    
    def test_restriction_size(self):
        """Test restriction size calculation."""
        from apache_beam.io.restriction_trackers import OffsetRange
        
        dofn = SplittableGeoTIFFReaderFn()
        restriction = OffsetRange(100, 500)
        size = dofn.restriction_size(self.test_file_path, restriction)
        
        self.assertEqual(size, 400)


class TestGeoTIFFTransform(unittest.TestCase):
    """Test cases for GeoTIFFTransform."""
    
    def test_initialization(self):
        """Test transform initialization."""
        transform = GeoTIFFTransform("*.tif", bands=[1, 2, 3], tile_size=(512, 512))
        
        self.assertEqual(transform.file_pattern, "*.tif")
        self.assertEqual(transform.bands, [1, 2, 3])
        self.assertEqual(transform.tile_size, (512, 512))
        self.assertEqual(transform.output_mode, 'tiles')
    
    def test_initialization_with_options(self):
        """Test transform initialization with options."""
        transform = GeoTIFFTransform(
            "*.tif",
            bands=[4, 5],
            tile_size=(1024, 1024),
            bbox=(-180, -90, 180, 90),
            output_mode='bands',
            use_splittable=False
        )
        
        self.assertEqual(transform.file_pattern, "*.tif")
        self.assertEqual(transform.bands, [4, 5])
        self.assertEqual(transform.tile_size, (1024, 1024))
        self.assertEqual(transform.bbox, (-180, -90, 180, 90))
        self.assertEqual(transform.output_mode, 'bands')
        self.assertFalse(transform.use_splittable)


@unittest.skipIf(rasterio is None, "Rasterio not available")
class TestRasterTransforms(unittest.TestCase):
    """Test cases for raster transforms."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.test_tiles = [
            RasterTile(
                data=np.random.randint(0, 1000, (4, 512, 512), dtype=np.uint16),
                window=Window(0, 0, 512, 512),
                transform=(0.1, 0, -180, 0, -0.1, 90),
                bands=[1, 2, 3, 4],
                nodata=None,
                tile_id="0_0"
            ),
            RasterTile(
                data=np.random.randint(0, 1000, (4, 512, 512), dtype=np.uint16),
                window=Window(512, 0, 512, 512),
                transform=(0.1, 0, -180, 0, -0.1, 90),
                bands=[1, 2, 3, 4],
                nodata=None,
                tile_id="0_1"
            )
        ]
    
    def test_raster_band_math_transform(self):
        """Test raster band math transform."""
        with TestPipeline() as p:
            # Create test data
            tiles = p | beam.Create(self.test_tiles)
            
            # Apply band math (NDVI calculation)
            ndvi_tiles = tiles | RasterBandMathTransform(
                expression="(B4 - B3) / (B4 + B3)",
                output_name="ndvi"
            )
            
            # Verify results
            def check_result(result):
                self.assertEqual(len(result), 2)
                for tile in result:
                    self.assertIsInstance(tile, RasterTile)
                    # Should have 1 band (NDVI result)
                    self.assertEqual(tile.data.shape[0], 1)
            
            assert_that(ndvi_tiles, check_result)


class TestIntegration(unittest.TestCase):
    """Integration tests for GeoTIFF reader."""
    
    @patch('gexus.formats.geotiff.GeoTIFFReader')
    def test_end_to_end_pipeline(self, mock_reader_class):
        """Test end-to-end pipeline with GeoTIFF reader."""
        # Mock reader
        mock_reader = Mock()
        mock_tiles = [
            RasterTile(
                data=np.random.randint(0, 1000, (3, 512, 512), dtype=np.uint16),
                window=Window(0, 0, 512, 512),
                transform=(0.1, 0, -180, 0, -0.1, 90),
                bands=[1, 2, 3],
                nodata=None,
                tile_id="0_0"
            )
        ]
        mock_reader.read_tiles.return_value = iter(mock_tiles)
        mock_reader_class.return_value = mock_reader
        
        # Create pipeline
        with TestPipeline() as p:
            # Mock file matching
            with patch('apache_beam.io.fileio.MatchFiles') as mock_match, \
                 patch('apache_beam.io.fileio.ReadMatches') as mock_read:
                
                mock_file_metadata = Mock()
                mock_file_metadata.path = '/tmp/test.tif'
                mock_file = Mock()
                mock_file.metadata = mock_file_metadata
                
                mock_match.return_value = beam.Create([mock_file])
                mock_read.return_value = beam.Create([mock_file])
                
                # Read tiles
                tiles = (
                    p
                    | 'Start' >> beam.Create([None])
                    | 'Read GeoTIFF' >> GeoTIFFTransform('*.tif', use_splittable=False)
                )
                
                # Verify results
                def check_tiles(result):
                    self.assertEqual(len(result), 1)
                    tile = result[0]
                    self.assertIsInstance(tile, RasterTile)
                    self.assertEqual(tile.tile_id, "0_0")
                
                assert_that(tiles, check_tiles)


if __name__ == '__main__':
    unittest.main()

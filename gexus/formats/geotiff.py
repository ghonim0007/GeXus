"""GeoTIFF format reader implementation for GeXus framework."""

import logging
import os
from typing import Dict, Iterator, Tuple, Any, Optional, List
import math

import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.io.restriction_trackers import OffsetRange
from apache_beam.transforms.core import DoFn
from apache_beam.transforms.util import Reshuffle

try:
    import numpy as np
except ImportError:
    raise ImportError(
        "numpy package is required. Install it with: pip install numpy>=1.21.0"
    )

try:
    import rasterio
    from rasterio.windows import Window
    from rasterio.crs import CRS
    from rasterio.transform import Affine
    from rasterio.enums import Resampling
except ImportError:
    raise ImportError(
        "rasterio package is required. Install it with: pip install rasterio>=1.3.0"
    )

try:
    import pyproj
except ImportError:
    raise ImportError(
        "pyproj package is required. Install it with: pip install pyproj>=3.4.0"
    )

from .raster_base import (
    BaseRasterReader, 
    BaseRasterTransform, 
    RasterBand, 
    RasterTile, 
    RasterMetadata,
    RasterWindow
)

logger = logging.getLogger(__name__)


class GeoTIFFReader(BaseRasterReader):
    """Production-ready GeoTIFF format reader with streaming capabilities."""
    
    def __init__(self, file_path: str, **kwargs):
        """Initialize GeoTIFF reader.
        
        Args:
            file_path: Path to the GeoTIFF file
            **kwargs: Additional options:
                - bands: List of band numbers to read (1-indexed)
                - bbox: Tuple of (left, bottom, right, top) for spatial filtering
                - overview_level: Overview level to read (0 for full resolution)
                - chunk_size: Size for chunked reading (default: 512)
                - validate_cog: Whether to validate Cloud-Optimized GeoTIFF structure
        """
        super().__init__(file_path, **kwargs)
        
        # Extract options
        self.target_bands = kwargs.get('bands')
        self.bbox = kwargs.get('bbox')
        self.overview_level = kwargs.get('overview_level', 0)
        self.chunk_size = kwargs.get('chunk_size', 512)
        self.validate_cog = kwargs.get('validate_cog', False)
        
        # Initialize metadata
        self._metadata = None
        self._src = None
        
        # Validate file and extract metadata
        self._initialize_reader()
    
    def _initialize_reader(self):
        """Initialize the GeoTIFF reader and cache metadata."""
        try:
            if not os.path.exists(self.file_path):
                raise FileNotFoundError(f"GeoTIFF file not found: {self.file_path}")
            
            # Open file and extract metadata
            with rasterio.open(self.file_path) as src:
                self._metadata = RasterMetadata(
                    width=src.width,
                    height=src.height,
                    count=src.count,
                    dtype=str(src.dtype),
                    crs=src.crs,
                    transform=src.transform.to_gdal(),
                    nodata=src.nodata,
                    bounds=src.bounds,
                    res=src.res,
                    units=src.crs.linear_units if src.crs else None,
                    compress=src.compression.name if src.compression else None,
                    interleave=src.interleaving.name if src.interleaving else None,
                    tiled=src.is_tiled,
                    blockxsize=src.block_shapes[0][1] if src.block_shapes else None,
                    blockysize=src.block_shapes[0][0] if src.block_shapes else None
                )
                
                # Validate COG if requested
                if self.validate_cog:
                    self._validate_cog_structure(src)
                    
        except Exception as e:
            logger.error(f"Failed to initialize GeoTIFF reader for {self.file_path}: {e}")
            raise
    
    def _validate_cog_structure(self, src):
        """Validate Cloud-Optimized GeoTIFF structure."""
        issues = []
        
        # Check if tiled
        if not src.is_tiled:
            issues.append("Not tiled")
        
        # Check overviews
        if not src.overviews(1):
            issues.append("No overviews")
        
        # Check block size
        if src.block_shapes and src.block_shapes[0] != (512, 512):
            issues.append(f"Non-standard block size: {src.block_shapes[0]}")
        
        if issues:
            logger.warning(f"COG validation issues for {self.file_path}: {', '.join(issues)}")
        else:
            logger.info(f"Valid COG structure: {self.file_path}")
    
    def read_bands(self, bands: Optional[List[int]] = None) -> Iterator[RasterBand]:
        """Read specified bands as iterator for memory efficiency.
        
        Args:
            bands: List of band numbers to read (1-indexed). None for all bands.
            
        Yields:
            RasterBand objects containing band data and metadata
        """
        try:
            with rasterio.open(self.file_path) as src:
                # Determine bands to read
                if bands is None:
                    bands = list(range(1, src.count + 1))
                
                # Apply target bands filter if specified
                if self.target_bands:
                    bands = [b for b in bands if b in self.target_bands]
                
                for band_num in bands:
                    if band_num > src.count:
                        logger.warning(f"Band {band_num} does not exist in {self.file_path}")
                        continue
                    
                    try:
                        # Read band data
                        if self.bbox:
                            # Read spatial subset
                            window = self._bbox_to_window(src, self.bbox)
                            data = src.read(band_num, window=window)
                        else:
                            # Read full band
                            data = src.read(band_num)
                        
                        # Get band metadata
                        band_meta = {
                            'description': src.descriptions[band_num - 1],
                            'color_interp': src.colorinterp[band_num - 1].name,
                            'units': src.units[band_num - 1] if src.units else None,
                            'offset': src.offsets[band_num - 1] if src.offsets else None,
                            'scale': src.scales[band_num - 1] if src.scales else None
                        }
                        
                        yield RasterBand(
                            data=data,
                            band_number=band_num,
                            nodata=src.nodata,
                            dtype=data.dtype,
                            description=band_meta['description'],
                            color_interp=band_meta['color_interp']
                        )
                        
                    except Exception as e:
                        logger.warning(f"Failed to read band {band_num}: {e}")
                        continue
                        
        except Exception as e:
            logger.error(f"Failed to read bands from {self.file_path}: {e}")
            raise
    
    def read_window(self, window: Window, bands: Optional[List[int]] = None) -> np.ndarray:
        """Read spatial window from raster.
        
        Args:
            window: Rasterio Window object defining spatial subset
            bands: List of band numbers to read (1-indexed)
            
        Returns:
            Numpy array with shape (bands, height, width)
        """
        try:
            with rasterio.open(self.file_path) as src:
                # Determine bands to read
                if bands is None:
                    bands = list(range(1, src.count + 1))
                
                # Apply target bands filter if specified
                if self.target_bands:
                    bands = [b for b in bands if b in self.target_bands]
                
                # Read data
                data = src.read(bands, window=window)
                
                return data
                
        except Exception as e:
            logger.error(f"Failed to read window from {self.file_path}: {e}")
            raise
    
    def read_tiles(self, tile_size: Tuple[int, int] = (512, 512), 
                   bands: Optional[List[int]] = None) -> Iterator[RasterTile]:
        """Read raster in tiles for distributed processing.
        
        Args:
            tile_size: Size of tiles as (width, height) in pixels
            bands: List of band numbers to read (1-indexed)
            
        Yields:
            RasterTile objects containing tile data and spatial information
        """
        try:
            with rasterio.open(self.file_path) as src:
                # Determine bands to read
                if bands is None:
                    bands = list(range(1, src.count + 1))
                
                # Apply target bands filter if specified
                if self.target_bands:
                    bands = [b for b in bands if b in self.target_bands]
                
                # Calculate tile grid
                tile_width, tile_height = tile_size
                cols = math.ceil(src.width / tile_width)
                rows = math.ceil(src.height / tile_height)
                
                for row in range(rows):
                    for col in range(cols):
                        # Calculate window
                        col_off = col * tile_width
                        row_off = row * tile_height
                        width = min(tile_width, src.width - col_off)
                        height = min(tile_height, src.height - row_off)
                        
                        window = Window(col_off, row_off, width, height)
                        
                        try:
                            # Read tile data
                            data = src.read(bands, window=window)
                            
                            # Calculate tile transform
                            tile_transform = rasterio.windows.transform(window, src.transform)
                            
                            # Create tile ID
                            tile_id = f"{row}_{col}"
                            
                            yield RasterTile(
                                data=data,
                                window=window,
                                transform=tile_transform.to_gdal(),
                                bands=bands,
                                nodata=src.nodata,
                                tile_id=tile_id
                            )
                            
                        except Exception as e:
                            logger.warning(f"Failed to read tile {tile_id}: {e}")
                            continue
                            
        except Exception as e:
            logger.error(f"Failed to read tiles from {self.file_path}: {e}")
            raise
    
    def _bbox_to_window(self, src, bbox: Tuple[float, float, float, float]) -> Window:
        """Convert bounding box to rasterio window."""
        try:
            from rasterio.windows import from_bounds
            return from_bounds(*bbox, transform=src.transform)
        except Exception as e:
            logger.error(f"Failed to convert bbox to window: {e}")
            raise
    
    def get_spatial_reference(self) -> CRS:
        """Extract coordinate reference system."""
        return self._metadata.crs if self._metadata else None
    
    def get_geo_transform(self) -> Tuple[float, ...]:
        """Get geotransform parameters."""
        return self._metadata.transform if self._metadata else None
    
    def get_metadata(self) -> Dict[str, Any]:
        """Extract raster metadata."""
        return self._metadata.to_dict() if self._metadata else {}
    
    def get_bounds(self) -> Tuple[float, float, float, float]:
        """Get spatial bounds of the raster."""
        if self._metadata and hasattr(self._metadata, 'bounds'):
            bounds = self._metadata.extra.get('bounds')
            if bounds:
                return (bounds.left, bounds.bottom, bounds.right, bounds.top)
        return (0.0, 0.0, 0.0, 0.0)
    
    def get_shape(self) -> Tuple[int, int, int]:
        """Get raster dimensions."""
        if self._metadata:
            return (self._metadata.count, self._metadata.height, self._metadata.width)
        return (0, 0, 0)
    
    def get_overviews(self, band: int = 1) -> List[int]:
        """Get overview levels for a band."""
        try:
            with rasterio.open(self.file_path) as src:
                return src.overviews(band)
        except Exception as e:
            logger.warning(f"Failed to get overviews: {e}")
            return []


class ReadGeoTIFFTilesFn(DoFn):
    """DoFn for reading GeoTIFF files as tiles."""
    
    def __init__(self, tile_size: Tuple[int, int] = (512, 512), **kwargs):
        """Initialize the DoFn with options.
        
        Args:
            tile_size: Size of tiles as (width, height) in pixels
            **kwargs: Options to pass to GeoTIFFReader
        """
        self.tile_size = tile_size
        self.options = kwargs
    
    def process(self, file_path: str):
        """Process a single GeoTIFF file.
        
        Args:
            file_path: Path to the GeoTIFF file
            
        Yields:
            RasterTile objects
        """
        try:
            reader = GeoTIFFReader(file_path, **self.options)
            
            for tile in reader.read_tiles(tile_size=self.tile_size):
                yield tile
                
        except Exception as e:
            logger.error(f"Failed to process file {file_path}: {e}")
            raise


class ReadGeoTIFFBandsFn(DoFn):
    """DoFn for reading GeoTIFF files as bands."""
    
    def __init__(self, **kwargs):
        """Initialize the DoFn with options.
        
        Args:
            **kwargs: Options to pass to GeoTIFFReader
        """
        self.options = kwargs
    
    def process(self, file_path: str):
        """Process a single GeoTIFF file.
        
        Args:
            file_path: Path to the GeoTIFF file
            
        Yields:
            RasterBand objects
        """
        try:
            reader = GeoTIFFReader(file_path, **self.options)
            
            for band in reader.read_bands():
                yield band
                
        except Exception as e:
            logger.error(f"Failed to process file {file_path}: {e}")
            raise


class SplittableGeoTIFFReaderFn(DoFn):
    """Splittable DoFn for parallel reading of large GeoTIFF files."""
    
    def __init__(self, tile_size: Tuple[int, int] = (512, 512), **kwargs):
        """Initialize the splittable DoFn with options.
        
        Args:
            tile_size: Size of tiles as (width, height) in pixels
            **kwargs: Options to pass to GeoTIFFReader
        """
        self.tile_size = tile_size
        self.options = kwargs
    
    def process(self, file_path: str, restriction_tracker=DoFn.RestrictionParam):
        """Process a portion of a GeoTIFF file.
        
        Args:
            file_path: Path to the GeoTIFF file
            restriction_tracker: Beam restriction tracker for splitting
            
        Yields:
            RasterTile objects
        """
        try:
            reader = GeoTIFFReader(file_path, **self.options)
            
            # Calculate total number of tiles
            shape = reader.get_shape()
            if len(shape) < 3:
                logger.warning(f"Invalid raster shape: {shape}")
                return
            
            _, height, width = shape
            tile_width, tile_height = self.tile_size
            total_tiles = math.ceil(height / tile_height) * math.ceil(width / tile_width)
            
            if total_tiles == 0:
                return
            
            # Process tiles within the restriction range
            current_tile = 0
            for tile in reader.read_tiles(tile_size=self.tile_size):
                if not restriction_tracker.try_claim(current_tile):
                    break
                
                yield tile
                current_tile += 1
                
        except Exception as e:
            logger.error(f"Failed to process file {file_path}: {e}")
            raise
    
    def create_initial_restriction(self, file_path: str) -> OffsetRange:
        """Create initial restriction for the file."""
        try:
            reader = GeoTIFFReader(file_path, **self.options)
            shape = reader.get_shape()
            
            if len(shape) < 3:
                return OffsetRange(0, 1)
            
            _, height, width = shape
            tile_width, tile_height = self.tile_size
            total_tiles = math.ceil(height / tile_height) * math.ceil(width / tile_width)
            
            return OffsetRange(0, max(1, total_tiles))
            
        except Exception as e:
            logger.warning(f"Failed to get tile count for {file_path}: {e}")
            return OffsetRange(0, 1)
    
    def split(self, file_path: str, restriction: OffsetRange) -> List[OffsetRange]:
        """Split the restriction into smaller chunks."""
        # Split into chunks of approximately 100 tiles each
        chunk_size = 100
        splits = []
        
        start = restriction.start
        end = restriction.stop
        
        while start < end:
            chunk_end = min(start + chunk_size, end)
            splits.append(OffsetRange(start, chunk_end))
            start = chunk_end
        
        return splits if len(splits) > 1 else [restriction]
    
    def restriction_size(self, file_path: str, restriction: OffsetRange) -> int:
        """Get the size of the restriction."""
        return restriction.stop - restriction.start


class GeoTIFFTransform(BaseRasterTransform):
    """Apache Beam transform for reading GeoTIFF files."""
    
    def __init__(self, 
                 file_pattern: str,
                 bands: Optional[List[int]] = None,
                 tile_size: Tuple[int, int] = (512, 512),
                 bbox: Optional[Tuple[float, ...]] = None,
                 use_overviews: bool = True,
                 output_mode: str = 'tiles',
                 use_splittable: bool = True,
                 **kwargs):
        """Initialize GeoTIFF transform.
        
        Args:
            file_pattern: File pattern to match (supports wildcards)
            bands: List of band numbers to read (1-indexed)
            tile_size: Size of tiles as (width, height) in pixels
            bbox: Bounding box as (left, bottom, right, top)
            use_overviews: Whether to use overview levels for efficiency
            output_mode: 'tiles' or 'bands' - how to output raster data
            use_splittable: Whether to use splittable DoFn for large files
            **kwargs: Additional options passed to GeoTIFFReader
        """
        super().__init__(file_pattern, **kwargs)
        self.bands = bands
        self.tile_size = tile_size
        self.bbox = bbox
        self.use_overviews = use_overviews
        self.output_mode = output_mode
        self.use_splittable = use_splittable
        
        # Add options to pass to reader
        self.options.update({
            'bands': bands,
            'bbox': bbox,
            'tile_size': tile_size
        })
    
    def expand(self, pcoll):
        """Expand the transform to read GeoTIFF files.
        
        Args:
            pcoll: Input PCollection (typically empty)
            
        Returns:
            PCollection of RasterTile or RasterBand objects
        """
        if self.output_mode == 'tiles':
            if self.use_splittable:
                return (
                    pcoll
                    | 'Match Files' >> fileio.MatchFiles(self.file_pattern)
                    | 'Read Matches' >> fileio.ReadMatches()
                    | 'Extract File Paths' >> beam.Map(lambda x: x.metadata.path)
                    | 'Read GeoTIFF Tiles Splittable' >> beam.ParDo(
                        SplittableGeoTIFFReaderFn(self.tile_size, **self.options)
                    )
                    | 'Reshuffle' >> Reshuffle()
                )
            else:
                return (
                    pcoll
                    | 'Match Files' >> fileio.MatchFiles(self.file_pattern)
                    | 'Read Matches' >> fileio.ReadMatches()
                    | 'Extract File Paths' >> beam.Map(lambda x: x.metadata.path)
                    | 'Read GeoTIFF Tiles' >> beam.ParDo(
                        ReadGeoTIFFTilesFn(self.tile_size, **self.options)
                    )
                    | 'Reshuffle' >> Reshuffle()
                )
        else:  # bands mode
            return (
                pcoll
                | 'Match Files' >> fileio.MatchFiles(self.file_pattern)
                | 'Read Matches' >> fileio.ReadMatches()
                | 'Extract File Paths' >> beam.Map(lambda x: x.metadata.path)
                | 'Read GeoTIFF Bands' >> beam.ParDo(
                    ReadGeoTIFFBandsFn(**self.options)
                )
                | 'Reshuffle' >> Reshuffle()
            )

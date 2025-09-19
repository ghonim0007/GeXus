"""Base interface for raster format handlers."""

from abc import ABC, abstractmethod
from typing import Dict, Iterator, Tuple, Any, Optional, List, NamedTuple
import numpy as np
import apache_beam as beam

try:
    from rasterio.windows import Window
    from rasterio.crs import CRS
except ImportError:
    # Fallback types for when rasterio is not available
    Window = Any
    CRS = Any


class RasterBand(NamedTuple):
    """Represents a single raster band with metadata."""
    data: np.ndarray
    band_number: int
    nodata: Optional[float]
    dtype: np.dtype
    description: Optional[str]
    color_interp: Optional[str]


class RasterTile(NamedTuple):
    """Represents a spatial tile from a raster."""
    data: np.ndarray
    window: Window
    transform: Tuple[float, ...]
    bands: List[int]
    nodata: Optional[float]
    tile_id: str


class BaseRasterReader(ABC):
    """Abstract base class for raster format readers."""
    
    def __init__(self, file_path: str, **kwargs):
        """Initialize reader with file path and optional parameters.
        
        Args:
            file_path: Path to the raster data file
            **kwargs: Additional format-specific parameters
        """
        self.file_path = file_path
        self.options = kwargs
    
    @abstractmethod
    def read_bands(self, bands: Optional[List[int]] = None) -> Iterator[RasterBand]:
        """Read specified bands as iterator for memory efficiency.
        
        Args:
            bands: List of band numbers to read (1-indexed). None for all bands.
            
        Yields:
            RasterBand objects containing band data and metadata
        """
        pass
    
    @abstractmethod
    def read_window(self, window: Window, bands: Optional[List[int]] = None) -> np.ndarray:
        """Read spatial window from raster.
        
        Args:
            window: Rasterio Window object defining spatial subset
            bands: List of band numbers to read (1-indexed)
            
        Returns:
            Numpy array with shape (bands, height, width)
        """
        pass
    
    @abstractmethod
    def read_tiles(self, tile_size: Tuple[int, int] = (512, 512), 
                   bands: Optional[List[int]] = None) -> Iterator[RasterTile]:
        """Read raster in tiles for distributed processing.
        
        Args:
            tile_size: Size of tiles as (width, height) in pixels
            bands: List of band numbers to read (1-indexed)
            
        Yields:
            RasterTile objects containing tile data and spatial information
        """
        pass
    
    @abstractmethod
    def get_spatial_reference(self) -> CRS:
        """Extract coordinate reference system.
        
        Returns:
            Rasterio CRS object
        """
        pass
    
    @abstractmethod
    def get_geo_transform(self) -> Tuple[float, ...]:
        """Get geotransform parameters (pixel to world coordinates).
        
        Returns:
            6-element tuple: (x_origin, pixel_width, x_rotation, 
                            y_origin, y_rotation, pixel_height)
        """
        pass
    
    @abstractmethod
    def get_metadata(self) -> Dict[str, Any]:
        """Extract raster metadata including band information.
        
        Returns:
            Dictionary containing raster metadata
        """
        pass
    
    @abstractmethod
    def get_bounds(self) -> Tuple[float, float, float, float]:
        """Get spatial bounds of the raster.
        
        Returns:
            Tuple of (left, bottom, right, top) in CRS units
        """
        pass
    
    @abstractmethod
    def get_shape(self) -> Tuple[int, int, int]:
        """Get raster dimensions.
        
        Returns:
            Tuple of (bands, height, width)
        """
        pass
    
    @abstractmethod
    def get_overviews(self, band: int = 1) -> List[int]:
        """Get overview levels for a band.
        
        Args:
            band: Band number (1-indexed)
            
        Returns:
            List of overview levels (decimation factors)
        """
        pass


class BaseRasterTransform(beam.PTransform, ABC):
    """Abstract base class for Apache Beam raster transforms."""
    
    def __init__(self, file_pattern: str, **kwargs):
        """Initialize transform with file pattern.
        
        Args:
            file_pattern: File pattern to match (supports wildcards)
            **kwargs: Additional format-specific parameters
        """
        super().__init__()
        self.file_pattern = file_pattern
        self.options = kwargs
    
    @abstractmethod
    def expand(self, pcoll):
        """Expand the transform to read raster files.
        
        Args:
            pcoll: Input PCollection (typically empty)
            
        Returns:
            PCollection of raster tiles or bands
        """
        pass


class RasterWindow:
    """Helper class for defining spatial windows."""
    
    def __init__(self, col_off: int, row_off: int, width: int, height: int):
        """Initialize raster window.
        
        Args:
            col_off: Column offset (left)
            row_off: Row offset (top)
            width: Window width in pixels
            height: Window height in pixels
        """
        self.col_off = col_off
        self.row_off = row_off
        self.width = width
        self.height = height
    
    def to_rasterio_window(self) -> Window:
        """Convert to rasterio Window object."""
        try:
            from rasterio.windows import Window
            return Window(self.col_off, self.row_off, self.width, self.height)
        except ImportError:
            raise ImportError("rasterio is required for window operations")
    
    @classmethod
    def from_bounds(cls, bounds: Tuple[float, float, float, float], 
                   transform: Tuple[float, ...]) -> 'RasterWindow':
        """Create window from spatial bounds.
        
        Args:
            bounds: Spatial bounds as (left, bottom, right, top)
            transform: Geotransform parameters
            
        Returns:
            RasterWindow object
        """
        try:
            from rasterio.windows import from_bounds
            from rasterio.transform import Affine
            
            affine_transform = Affine.from_gdal(*transform)
            window = from_bounds(*bounds, transform=affine_transform)
            
            return cls(
                col_off=int(window.col_off),
                row_off=int(window.row_off),
                width=int(window.width),
                height=int(window.height)
            )
        except ImportError:
            raise ImportError("rasterio is required for bounds-based window creation")


class RasterMetadata:
    """Container for raster metadata."""
    
    def __init__(self, 
                 width: int,
                 height: int,
                 count: int,
                 dtype: str,
                 crs: Optional[CRS] = None,
                 transform: Optional[Tuple[float, ...]] = None,
                 nodata: Optional[float] = None,
                 **kwargs):
        """Initialize raster metadata.
        
        Args:
            width: Raster width in pixels
            height: Raster height in pixels
            count: Number of bands
            dtype: Data type as string
            crs: Coordinate reference system
            transform: Geotransform parameters
            nodata: Nodata value
            **kwargs: Additional metadata
        """
        self.width = width
        self.height = height
        self.count = count
        self.dtype = dtype
        self.crs = crs
        self.transform = transform
        self.nodata = nodata
        self.extra = kwargs
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert metadata to dictionary."""
        result = {
            'width': self.width,
            'height': self.height,
            'count': self.count,
            'dtype': self.dtype,
            'nodata': self.nodata
        }
        
        if self.crs:
            result['crs'] = str(self.crs)
        
        if self.transform:
            result['transform'] = self.transform
        
        result.update(self.extra)
        return result

"""Raster transforms for Apache Beam geospatial processing."""

import logging
from typing import Dict, Any, Tuple, Optional, List, Callable
import math

import apache_beam as beam
from apache_beam.transforms.core import DoFn

try:
    import numpy as np
except ImportError:
    raise ImportError(
        "numpy package is required. Install it with: pip install numpy>=1.21.0"
    )

try:
    import rasterio
    from rasterio.enums import Resampling
    from rasterio.warp import reproject, calculate_default_transform
    from rasterio.crs import CRS
except ImportError:
    raise ImportError(
        "rasterio package is required. Install it with: pip install rasterio>=1.3.0"
    )

try:
    from shapely.geometry import shape, mapping
    from shapely.ops import transform
except ImportError:
    raise ImportError(
        "shapely package is required. Install it with: pip install shapely>=1.8.0"
    )

from ..formats.raster_base import RasterTile, RasterBand

logger = logging.getLogger(__name__)


class RasterBandMathFn(DoFn):
    """DoFn for performing band math operations on raster data."""
    
    def __init__(self, expression: str, output_name: str = "result"):
        """Initialize band math function.
        
        Args:
            expression: Mathematical expression using band names (e.g., "(B4 - B3) / (B4 + B3)")
            output_name: Name for the output band
        """
        self.expression = expression
        self.output_name = output_name
        self._compiled_expr = None
        
        # Compile expression for performance
        self._compile_expression()
    
    def _compile_expression(self):
        """Compile the mathematical expression."""
        try:
            # Replace band references with array indexing
            expr = self.expression
            # Simple replacement for common patterns
            expr = expr.replace('B1', 'bands[0]')
            expr = expr.replace('B2', 'bands[1]')
            expr = expr.replace('B3', 'bands[2]')
            expr = expr.replace('B4', 'bands[3]')
            expr = expr.replace('B5', 'bands[4]')
            expr = expr.replace('B6', 'bands[5]')
            expr = expr.replace('B7', 'bands[6]')
            expr = expr.replace('B8', 'bands[7]')
            
            self._compiled_expr = expr
            logger.info(f"Compiled expression: {self._compiled_expr}")
            
        except Exception as e:
            logger.error(f"Failed to compile expression {self.expression}: {e}")
            raise
    
    def process(self, element):
        """Process raster element with band math.
        
        Args:
            element: RasterTile or RasterBand object
            
        Yields:
            Modified raster element with calculated band
        """
        try:
            if isinstance(element, RasterTile):
                bands = element.data
            elif isinstance(element, RasterBand):
                bands = element.data[np.newaxis, ...]  # Add band dimension
            else:
                logger.warning(f"Unsupported element type: {type(element)}")
                return
            
            # Ensure we have enough bands
            if bands.shape[0] < 8:  # Pad with zeros if needed
                padded_bands = np.zeros((8, bands.shape[1], bands.shape[2]), dtype=bands.dtype)
                padded_bands[:bands.shape[0]] = bands
                bands = padded_bands
            
            # Calculate result using the compiled expression
            try:
                # Use numpy's safe evaluation
                result = eval(self._compiled_expr, {"__builtins__": {}}, {
                    "bands": bands,
                    "np": np,
                    "numpy": np
                })
                
                # Handle division by zero and invalid values
                result = np.where(np.isfinite(result), result, np.nan)
                
                if isinstance(element, RasterTile):
                    # Create new tile with result
                    new_data = result[np.newaxis, ...] if result.ndim == 2 else result
                    yield element._replace(data=new_data)
                else:
                    # Create new band with result
                    yield RasterBand(
                        data=result,
                        band_number=1,
                        nodata=np.nan,
                        dtype=result.dtype,
                        description=self.output_name,
                        color_interp=None
                    )
                    
            except Exception as e:
                logger.error(f"Failed to evaluate expression: {e}")
                yield element  # Return original on error
                
        except Exception as e:
            logger.error(f"Band math processing failed: {e}")
            yield element


class RasterResampleFn(DoFn):
    """DoFn for resampling raster data to different resolution."""
    
    def __init__(self, target_resolution: float, method: str = 'bilinear'):
        """Initialize resampling function.
        
        Args:
            target_resolution: Target pixel resolution
            method: Resampling method ('nearest', 'bilinear', 'cubic', etc.)
        """
        self.target_resolution = target_resolution
        self.method = getattr(Resampling, method, Resampling.bilinear)
    
    def process(self, element):
        """Process raster element with resampling.
        
        Args:
            element: RasterTile object
            
        Yields:
            Resampled RasterTile object
        """
        try:
            if not isinstance(element, RasterTile):
                logger.warning(f"Resampling only supports RasterTile, got {type(element)}")
                yield element
                return
            
            # Calculate new dimensions
            current_transform = rasterio.transform.from_gdal(*element.transform)
            current_resolution = abs(current_transform.a)
            
            scale_factor = current_resolution / self.target_resolution
            new_height = int(element.data.shape[1] * scale_factor)
            new_width = int(element.data.shape[2] * scale_factor)
            
            # Create new transform
            new_transform = rasterio.transform.Affine(
                self.target_resolution, 0, current_transform.c,
                0, -self.target_resolution, current_transform.f
            )
            
            # Resample data
            resampled_data = np.zeros((element.data.shape[0], new_height, new_width), 
                                    dtype=element.data.dtype)
            
            reproject(
                element.data,
                resampled_data,
                src_transform=current_transform,
                dst_transform=new_transform,
                src_crs='EPSG:4326',  # Assume WGS84 for now
                dst_crs='EPSG:4326',
                resampling=self.method
            )
            
            yield element._replace(
                data=resampled_data,
                transform=new_transform.to_gdal()
            )
            
        except Exception as e:
            logger.error(f"Resampling failed: {e}")
            yield element


class ZonalStatisticsFn(DoFn):
    """DoFn for calculating zonal statistics."""
    
    def __init__(self, stats: List[str] = ['mean', 'std', 'count']):
        """Initialize zonal statistics function.
        
        Args:
            stats: List of statistics to calculate ('mean', 'std', 'count', 'min', 'max', 'sum')
        """
        self.stats = stats
    
    def process(self, element):
        """Process combined vector-raster element.
        
        Args:
            element: Tuple of (vector_feature, raster_tiles)
            
        Yields:
            Dictionary with calculated statistics
        """
        try:
            vector_feature, raster_tiles = element
            
            if not raster_tiles:
                yield {
                    'feature_id': vector_feature.get('properties', {}).get('id', 'unknown'),
                    'error': 'No raster data found'
                }
                return
            
            # Extract geometry
            geometry = shape(vector_feature['geometry'])
            
            # Collect all pixel values within the geometry
            all_values = []
            
            for tile in raster_tiles:
                if not isinstance(tile, RasterTile):
                    continue
                
                try:
                    # Create pixel coordinates
                    transform = rasterio.transform.from_gdal(*tile.transform)
                    height, width = tile.data.shape[1], tile.data.shape[2]
                    
                    # Create meshgrid of pixel coordinates
                    cols, rows = np.meshgrid(np.arange(width), np.arange(height))
                    xs, ys = rasterio.transform.xy(transform, rows, cols)
                    
                    # Convert to points and check intersection
                    from shapely.geometry import Point
                    
                    for i in range(height):
                        for j in range(width):
                            point = Point(xs[i][j], ys[i][j])
                            if geometry.contains(point):
                                # Add pixel values from all bands
                                for band_idx in range(tile.data.shape[0]):
                                    value = tile.data[band_idx, i, j]
                                    if not np.isnan(value) and value != tile.nodata:
                                        all_values.append(value)
                
                except Exception as e:
                    logger.warning(f"Failed to process tile {tile.tile_id}: {e}")
                    continue
            
            # Calculate statistics
            if not all_values:
                result = {
                    'feature_id': vector_feature.get('properties', {}).get('id', 'unknown'),
                    'pixel_count': 0
                }
                for stat in self.stats:
                    result[stat] = None
                yield result
                return
            
            values_array = np.array(all_values)
            result = {
                'feature_id': vector_feature.get('properties', {}).get('id', 'unknown'),
                'pixel_count': len(all_values)
            }
            
            for stat in self.stats:
                try:
                    if stat == 'mean':
                        result[stat] = float(np.mean(values_array))
                    elif stat == 'std':
                        result[stat] = float(np.std(values_array))
                    elif stat == 'count':
                        result[stat] = len(all_values)
                    elif stat == 'min':
                        result[stat] = float(np.min(values_array))
                    elif stat == 'max':
                        result[stat] = float(np.max(values_array))
                    elif stat == 'sum':
                        result[stat] = float(np.sum(values_array))
                    elif stat == 'median':
                        result[stat] = float(np.median(values_array))
                    else:
                        logger.warning(f"Unknown statistic: {stat}")
                        result[stat] = None
                except Exception as e:
                    logger.warning(f"Failed to calculate {stat}: {e}")
                    result[stat] = None
            
            yield result
            
        except Exception as e:
            logger.error(f"Zonal statistics calculation failed: {e}")
            yield {
                'feature_id': 'unknown',
                'error': str(e)
            }


class RasterFilterFn(DoFn):
    """DoFn for filtering raster data based on criteria."""
    
    def __init__(self, filter_func: Callable[[np.ndarray], bool]):
        """Initialize raster filter.
        
        Args:
            filter_func: Function that takes numpy array and returns boolean
        """
        self.filter_func = filter_func
    
    def process(self, element):
        """Process raster element with filtering.
        
        Args:
            element: RasterTile or RasterBand object
            
        Yields:
            Element if it passes the filter
        """
        try:
            if isinstance(element, (RasterTile, RasterBand)):
                if self.filter_func(element.data):
                    yield element
            else:
                logger.warning(f"Unsupported element type for filtering: {type(element)}")
                
        except Exception as e:
            logger.error(f"Raster filtering failed: {e}")


# Transform classes
class RasterBandMathTransform(beam.PTransform):
    """Transform for performing band math operations."""
    
    def __init__(self, expression: str, output_name: str = "result"):
        """Initialize band math transform.
        
        Args:
            expression: Mathematical expression using band names
            output_name: Name for the output band
        """
        super().__init__()
        self.expression = expression
        self.output_name = output_name
    
    def expand(self, pcoll):
        """Apply band math to raster data.
        
        Args:
            pcoll: PCollection of RasterTile or RasterBand objects
            
        Returns:
            PCollection of processed raster data
        """
        return pcoll | beam.ParDo(RasterBandMathFn(self.expression, self.output_name))


class RasterResampleTransform(beam.PTransform):
    """Transform for resampling raster data."""
    
    def __init__(self, target_resolution: float, method: str = 'bilinear'):
        """Initialize resampling transform.
        
        Args:
            target_resolution: Target pixel resolution
            method: Resampling method
        """
        super().__init__()
        self.target_resolution = target_resolution
        self.method = method
    
    def expand(self, pcoll):
        """Apply resampling to raster data.
        
        Args:
            pcoll: PCollection of RasterTile objects
            
        Returns:
            PCollection of resampled raster data
        """
        return pcoll | beam.ParDo(RasterResampleFn(self.target_resolution, self.method))


class ZonalStatisticsTransform(beam.PTransform):
    """Transform for calculating zonal statistics."""
    
    def __init__(self, vector_boundaries: beam.PCollection, stats: List[str] = ['mean', 'std', 'count']):
        """Initialize zonal statistics transform.
        
        Args:
            vector_boundaries: PCollection of vector features
            stats: List of statistics to calculate
        """
        super().__init__()
        self.vector_boundaries = vector_boundaries
        self.stats = stats
    
    def expand(self, pcoll):
        """Calculate zonal statistics.
        
        Args:
            pcoll: PCollection of RasterTile objects
            
        Returns:
            PCollection of statistics dictionaries
        """
        # Group raster tiles by spatial key (simplified)
        raster_keyed = pcoll | 'Key Raster by Tile' >> beam.Map(
            lambda tile: (tile.tile_id, tile)
        )
        
        # Key vector features by spatial intersection (simplified)
        vector_keyed = self.vector_boundaries | 'Key Vector by Feature' >> beam.Map(
            lambda feature: (feature.get('properties', {}).get('id', 'unknown'), feature)
        )
        
        # Combine and calculate statistics
        return (
            ({'vector': vector_keyed, 'raster': raster_keyed})
            | 'Group by Key' >> beam.CoGroupByKey()
            | 'Calculate Zonal Stats' >> beam.ParDo(ZonalStatisticsFn(self.stats))
        )


class RasterFilterTransform(beam.PTransform):
    """Transform for filtering raster data."""
    
    def __init__(self, filter_func: Callable[[np.ndarray], bool]):
        """Initialize raster filter transform.
        
        Args:
            filter_func: Function that takes numpy array and returns boolean
        """
        super().__init__()
        self.filter_func = filter_func
    
    def expand(self, pcoll):
        """Apply filtering to raster data.
        
        Args:
            pcoll: PCollection of raster data
            
        Returns:
            PCollection of filtered raster data
        """
        return pcoll | beam.ParDo(RasterFilterFn(self.filter_func))


class RasterMosaicFn(DoFn):
    """DoFn for mosaicking multiple raster tiles."""
    
    def __init__(self, output_bounds: Tuple[float, float, float, float], 
                 output_resolution: float):
        """Initialize mosaic function.
        
        Args:
            output_bounds: Output bounds as (left, bottom, right, top)
            output_resolution: Output pixel resolution
        """
        self.output_bounds = output_bounds
        self.output_resolution = output_resolution
    
    def process(self, tiles):
        """Process multiple tiles into a mosaic.
        
        Args:
            tiles: Iterable of RasterTile objects
            
        Yields:
            Mosaicked RasterTile object
        """
        try:
            tiles_list = list(tiles)
            if not tiles_list:
                return
            
            # Calculate output dimensions
            left, bottom, right, top = self.output_bounds
            width = int((right - left) / self.output_resolution)
            height = int((top - bottom) / self.output_resolution)
            
            # Get number of bands from first tile
            num_bands = tiles_list[0].data.shape[0]
            
            # Initialize output array
            output_data = np.full((num_bands, height, width), np.nan, dtype=np.float32)
            
            # Create output transform
            output_transform = rasterio.transform.from_bounds(
                left, bottom, right, top, width, height
            )
            
            # Mosaic tiles
            for tile in tiles_list:
                try:
                    # Calculate tile position in output grid
                    tile_transform = rasterio.transform.from_gdal(*tile.transform)
                    
                    # Simple nearest neighbor placement (can be improved)
                    tile_left = tile_transform.c
                    tile_top = tile_transform.f
                    
                    # Calculate pixel positions
                    col_start = int((tile_left - left) / self.output_resolution)
                    row_start = int((top - tile_top) / self.output_resolution)
                    
                    # Ensure bounds
                    col_start = max(0, min(col_start, width))
                    row_start = max(0, min(row_start, height))
                    
                    tile_height, tile_width = tile.data.shape[1], tile.data.shape[2]
                    col_end = min(col_start + tile_width, width)
                    row_end = min(row_start + tile_height, height)
                    
                    if col_end > col_start and row_end > row_start:
                        # Copy data
                        src_height = row_end - row_start
                        src_width = col_end - col_start
                        
                        output_data[:, row_start:row_end, col_start:col_end] = \
                            tile.data[:, :src_height, :src_width]
                
                except Exception as e:
                    logger.warning(f"Failed to mosaic tile {tile.tile_id}: {e}")
                    continue
            
            # Create output tile
            yield RasterTile(
                data=output_data,
                window=rasterio.windows.Window(0, 0, width, height),
                transform=output_transform.to_gdal(),
                bands=list(range(1, num_bands + 1)),
                nodata=np.nan,
                tile_id="mosaic"
            )
            
        except Exception as e:
            logger.error(f"Mosaicking failed: {e}")


class RasterMosaicTransform(beam.PTransform):
    """Transform for mosaicking raster tiles."""
    
    def __init__(self, output_bounds: Tuple[float, float, float, float], 
                 output_resolution: float):
        """Initialize mosaic transform.
        
        Args:
            output_bounds: Output bounds as (left, bottom, right, top)
            output_resolution: Output pixel resolution
        """
        super().__init__()
        self.output_bounds = output_bounds
        self.output_resolution = output_resolution
    
    def expand(self, pcoll):
        """Create mosaic from raster tiles.
        
        Args:
            pcoll: PCollection of RasterTile objects
            
        Returns:
            PCollection with single mosaicked RasterTile
        """
        return (
            pcoll
            | 'Group All Tiles' >> beam.GroupGlobally()
            | 'Create Mosaic' >> beam.ParDo(
                RasterMosaicFn(self.output_bounds, self.output_resolution)
            )
        )

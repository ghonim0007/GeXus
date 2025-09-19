"""Spatial transforms for Apache Beam geospatial processing."""

import logging
from typing import Dict, Any, Tuple, Optional, List

import apache_beam as beam
from apache_beam.transforms.core import DoFn

try:
    import shapely.geometry
    from shapely.geometry import shape, mapping, box
    from shapely.ops import transform
except ImportError:
    raise ImportError(
        "shapely package is required. Install it with: pip install shapely>=1.8.0"
    )

try:
    import pyproj
    from pyproj import Transformer
except ImportError:
    raise ImportError(
        "pyproj package is required. Install it with: pip install pyproj>=3.4.0"
    )

logger = logging.getLogger(__name__)


class SpatialFilterFn(DoFn):
    """DoFn for filtering features based on spatial criteria."""
    
    def __init__(self, bbox: Optional[Tuple[float, float, float, float]] = None,
                 geometry_filter: Optional[Dict[str, Any]] = None):
        """Initialize spatial filter.
        
        Args:
            bbox: Bounding box as (minx, miny, maxx, maxy)
            geometry_filter: GeoJSON geometry for spatial filtering
        """
        self.bbox = bbox
        self.geometry_filter = geometry_filter
        self._filter_geom = None
        
        if geometry_filter:
            self._filter_geom = shape(geometry_filter)
    
    def process(self, feature: Dict[str, Any]):
        """Filter feature based on spatial criteria.
        
        Args:
            feature: Feature dictionary with geometry and properties
            
        Yields:
            Feature if it passes spatial filter
        """
        try:
            if not feature.get('geometry'):
                return
            
            geom = shape(feature['geometry'])
            
            # Apply bounding box filter
            if self.bbox:
                bbox_geom = box(*self.bbox)
                if not geom.intersects(bbox_geom):
                    return
            
            # Apply geometry filter
            if self._filter_geom:
                if not geom.intersects(self._filter_geom):
                    return
            
            yield feature
            
        except Exception as e:
            logger.warning(f"Spatial filter failed for feature: {e}")


class GeometryValidationFn(DoFn):
    """DoFn for validating and fixing geometries."""
    
    def __init__(self, fix_invalid: bool = True, remove_invalid: bool = False):
        """Initialize geometry validation.
        
        Args:
            fix_invalid: Whether to attempt fixing invalid geometries
            remove_invalid: Whether to remove invalid geometries from output
        """
        self.fix_invalid = fix_invalid
        self.remove_invalid = remove_invalid
    
    def process(self, feature: Dict[str, Any]):
        """Validate and optionally fix geometry.
        
        Args:
            feature: Feature dictionary with geometry and properties
            
        Yields:
            Feature with validated/fixed geometry
        """
        try:
            if not feature.get('geometry'):
                yield feature
                return
            
            geom = shape(feature['geometry'])
            
            if not geom.is_valid:
                logger.warning("Invalid geometry detected")
                
                if self.fix_invalid:
                    # Try to fix the geometry
                    fixed_geom = geom.buffer(0)
                    if fixed_geom.is_valid:
                        feature['geometry'] = mapping(fixed_geom)
                        logger.info("Geometry fixed successfully")
                    elif self.remove_invalid:
                        logger.warning("Could not fix geometry, removing feature")
                        return
                elif self.remove_invalid:
                    logger.warning("Invalid geometry, removing feature")
                    return
            
            yield feature
            
        except Exception as e:
            logger.error(f"Geometry validation failed: {e}")
            if not self.remove_invalid:
                yield feature


class CRSTransformFn(DoFn):
    """DoFn for coordinate reference system transformations."""
    
    def __init__(self, source_crs: str, target_crs: str):
        """Initialize CRS transformation.
        
        Args:
            source_crs: Source CRS (EPSG code or WKT)
            target_crs: Target CRS (EPSG code or WKT)
        """
        self.source_crs = source_crs
        self.target_crs = target_crs
        self._transformer = None
    
    def setup(self):
        """Setup the transformer."""
        try:
            self._transformer = Transformer.from_crs(
                self.source_crs, self.target_crs, always_xy=True
            )
        except Exception as e:
            logger.error(f"Failed to create CRS transformer: {e}")
            raise
    
    def process(self, feature: Dict[str, Any]):
        """Transform feature geometry to target CRS.
        
        Args:
            feature: Feature dictionary with geometry and properties
            
        Yields:
            Feature with transformed geometry
        """
        try:
            if not feature.get('geometry') or not self._transformer:
                yield feature
                return
            
            geom = shape(feature['geometry'])
            
            # Transform geometry
            transformed_geom = transform(self._transformer.transform, geom)
            feature['geometry'] = mapping(transformed_geom)
            
            yield feature
            
        except Exception as e:
            logger.error(f"CRS transformation failed: {e}")
            yield feature  # Return original feature on error


class BoundsCalculationFn(DoFn):
    """DoFn for calculating spatial bounds of features."""
    
    def process(self, feature: Dict[str, Any]):
        """Calculate bounds for feature geometry.
        
        Args:
            feature: Feature dictionary with geometry and properties
            
        Yields:
            Tuple of (feature, bounds) where bounds is (minx, miny, maxx, maxy)
        """
        try:
            if not feature.get('geometry'):
                yield (feature, None)
                return
            
            geom = shape(feature['geometry'])
            bounds = geom.bounds  # Returns (minx, miny, maxx, maxy)
            
            yield (feature, bounds)
            
        except Exception as e:
            logger.warning(f"Bounds calculation failed: {e}")
            yield (feature, None)


class SpatialFilterTransform(beam.PTransform):
    """Transform for filtering features based on spatial criteria."""
    
    def __init__(self, bbox: Optional[Tuple[float, float, float, float]] = None,
                 geometry_filter: Optional[Dict[str, Any]] = None):
        """Initialize spatial filter transform.
        
        Args:
            bbox: Bounding box as (minx, miny, maxx, maxy)
            geometry_filter: GeoJSON geometry for spatial filtering
        """
        super().__init__()
        self.bbox = bbox
        self.geometry_filter = geometry_filter
    
    def expand(self, pcoll):
        """Apply spatial filter to features.
        
        Args:
            pcoll: PCollection of feature dictionaries
            
        Returns:
            PCollection of filtered features
        """
        return pcoll | beam.ParDo(SpatialFilterFn(self.bbox, self.geometry_filter))


class GeometryValidationTransform(beam.PTransform):
    """Transform for validating and fixing geometries."""
    
    def __init__(self, fix_invalid: bool = True, remove_invalid: bool = False):
        """Initialize geometry validation transform.
        
        Args:
            fix_invalid: Whether to attempt fixing invalid geometries
            remove_invalid: Whether to remove invalid geometries from output
        """
        super().__init__()
        self.fix_invalid = fix_invalid
        self.remove_invalid = remove_invalid
    
    def expand(self, pcoll):
        """Apply geometry validation to features.
        
        Args:
            pcoll: PCollection of feature dictionaries
            
        Returns:
            PCollection of validated features
        """
        return pcoll | beam.ParDo(GeometryValidationFn(self.fix_invalid, self.remove_invalid))


class CRSTransformTransform(beam.PTransform):
    """Transform for coordinate reference system transformations."""
    
    def __init__(self, source_crs: str, target_crs: str):
        """Initialize CRS transformation transform.
        
        Args:
            source_crs: Source CRS (EPSG code or WKT)
            target_crs: Target CRS (EPSG code or WKT)
        """
        super().__init__()
        self.source_crs = source_crs
        self.target_crs = target_crs
    
    def expand(self, pcoll):
        """Apply CRS transformation to features.
        
        Args:
            pcoll: PCollection of feature dictionaries
            
        Returns:
            PCollection of transformed features
        """
        return pcoll | beam.ParDo(CRSTransformFn(self.source_crs, self.target_crs))


class BoundsCalculationTransform(beam.PTransform):
    """Transform for calculating spatial bounds of features."""
    
    def expand(self, pcoll):
        """Calculate bounds for features.
        
        Args:
            pcoll: PCollection of feature dictionaries
            
        Returns:
            PCollection of (feature, bounds) tuples
        """
        return pcoll | beam.ParDo(BoundsCalculationFn())

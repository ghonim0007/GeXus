"""FlatGeobuf format reader implementation for GeXus framework."""

import logging
import os
from typing import Dict, Iterator, Tuple, Any, Optional, List
import json

import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.io.restriction_trackers import OffsetRange
from apache_beam.transforms.core import DoFn
from apache_beam.transforms.util import Reshuffle
from apache_beam.transforms.core import RestrictionProvider
from apache_beam.io.restriction_trackers import OffsetRestrictionTracker

try:
    import flatgeobuf
    from flatgeobuf import Reader
except ImportError:
    raise ImportError(
        "flatgeobuf package is required. Install it with: pip install flatgeobuf>=2.0.0"
    )

try:
    import shapely.geometry
    from shapely.geometry import shape
except ImportError:
    raise ImportError(
        "shapely package is required. Install it with: pip install shapely>=1.8.0"
    )

try:
    import pyproj
except ImportError:
    raise ImportError(
        "pyproj package is required. Install it with: pip install pyproj>=3.4.0"
    )

from .base import BaseFormatReader, BaseFormatTransform

logger = logging.getLogger(__name__)


class FlatGeobufReader(BaseFormatReader):
    """Production-ready FlatGeobuf format reader with streaming capabilities."""
    
    def __init__(self, file_path: str, **kwargs):
        """Initialize FlatGeobuf reader.
        
        Args:
            file_path: Path to the FlatGeobuf file
            **kwargs: Additional options:
                - bbox: Tuple of (minx, miny, maxx, maxy) for spatial filtering
                - properties: List of property names to include (None for all)
                - validate_geometry: Whether to validate geometries (default: True)
                - encoding: Text encoding (default: 'utf-8')
        """
        super().__init__(file_path, **kwargs)
        self._reader = None
        self._schema = None
        self._bounds = None
        self._feature_count = None
        
        # Extract options
        self.bbox = kwargs.get('bbox')
        self.properties = kwargs.get('properties')
        self.validate_geometry = kwargs.get('validate_geometry', True)
        self.encoding = kwargs.get('encoding', 'utf-8')
        
        # Initialize reader
        self._initialize_reader()
    
    def _initialize_reader(self):
        """Initialize the FlatGeobuf reader and cache metadata."""
        try:
            if not os.path.exists(self.file_path):
                raise FileNotFoundError(f"FlatGeobuf file not found: {self.file_path}")
            
            # Open the file and read header
            with open(self.file_path, 'rb') as f:
                self._reader = Reader(f)
                
                # Cache schema information
                header = self._reader.header()
                self._schema = self._extract_schema(header)
                
                # Cache bounds
                envelope = header.envelope()
                if envelope:
                    self._bounds = (
                        envelope.min_x(), envelope.min_y(),
                        envelope.max_x(), envelope.max_y()
                    )
                else:
                    self._bounds = (0.0, 0.0, 0.0, 0.0)
                
                # Cache feature count
                self._feature_count = header.features_count()
                
        except Exception as e:
            logger.error(f"Failed to initialize FlatGeobuf reader for {self.file_path}: {e}")
            raise
    
    def _extract_schema(self, header) -> Dict[str, Any]:
        """Extract schema information from FlatGeobuf header.
        
        Args:
            header: FlatGeobuf header object
            
        Returns:
            Schema dictionary with geometry type, properties, and CRS info
        """
        schema = {
            'geometry_type': self._get_geometry_type(header.geometry_type()),
            'properties': {},
            'crs': None
        }
        
        # Extract property schema
        for i in range(header.columns_length()):
            column = header.columns(i)
            if column:
                prop_name = column.name().decode(self.encoding) if column.name() else f"property_{i}"
                prop_type = self._get_property_type(column.type())
                schema['properties'][prop_name] = prop_type
        
        # Extract CRS information
        if header.crs():
            try:
                crs_data = header.crs()
                if hasattr(crs_data, 'wkt') and crs_data.wkt():
                    schema['crs'] = {'wkt': crs_data.wkt().decode(self.encoding)}
                elif hasattr(crs_data, 'code') and crs_data.code():
                    schema['crs'] = {'epsg': crs_data.code()}
            except Exception as e:
                logger.warning(f"Failed to extract CRS information: {e}")
        
        return schema
    
    def _get_geometry_type(self, geom_type) -> str:
        """Convert FlatGeobuf geometry type to string."""
        type_mapping = {
            0: 'Unknown',
            1: 'Point',
            2: 'LineString',
            3: 'Polygon',
            4: 'MultiPoint',
            5: 'MultiLineString',
            6: 'MultiPolygon',
            7: 'GeometryCollection',
            8: 'CircularString',
            9: 'CompoundCurve',
            10: 'CurvePolygon',
            11: 'MultiCurve',
            12: 'MultiSurface',
            13: 'Curve',
            14: 'Surface',
            15: 'PolyhedralSurface',
            16: 'TIN',
            17: 'Triangle'
        }
        return type_mapping.get(geom_type, 'Unknown')
    
    def _get_property_type(self, prop_type) -> str:
        """Convert FlatGeobuf property type to string."""
        type_mapping = {
            0: 'byte',
            1: 'ubyte',
            2: 'bool',
            3: 'short',
            4: 'ushort',
            5: 'int',
            6: 'uint',
            7: 'long',
            8: 'ulong',
            9: 'float',
            10: 'double',
            11: 'string',
            12: 'binary'
        }
        return type_mapping.get(prop_type, 'unknown')
    
    def read_features(self) -> Iterator[Dict[str, Any]]:
        """Yield features one by one for memory efficiency.
        
        Yields:
            Feature dictionaries with 'geometry' and 'properties' keys
        """
        try:
            with open(self.file_path, 'rb') as f:
                reader = Reader(f)
                
                # Apply spatial filter if bbox is provided
                if self.bbox:
                    features_iter = reader.filter_bbox(*self.bbox)
                else:
                    features_iter = reader
                
                for feature in features_iter:
                    try:
                        # Extract geometry
                        geometry = self._extract_geometry(feature)
                        
                        # Extract properties
                        properties = self._extract_properties(feature)
                        
                        # Filter properties if specified
                        if self.properties:
                            properties = {
                                k: v for k, v in properties.items() 
                                if k in self.properties
                            }
                        
                        yield {
                            'geometry': geometry,
                            'properties': properties,
                            'type': 'Feature'
                        }
                        
                    except Exception as e:
                        logger.warning(f"Failed to process feature: {e}")
                        if not self.validate_geometry:
                            continue
                        raise
                        
        except Exception as e:
            logger.error(f"Failed to read features from {self.file_path}: {e}")
            raise
    
    def _extract_geometry(self, feature) -> Optional[Dict[str, Any]]:
        """Extract geometry from FlatGeobuf feature.
        
        Args:
            feature: FlatGeobuf feature object
            
        Returns:
            GeoJSON-style geometry dictionary or None
        """
        try:
            # Get geometry as GeoJSON directly from FlatGeobuf
            geom_dict = feature.geometry_geojson()
            if not geom_dict:
                return None
            
            # Parse JSON if it's a string
            if isinstance(geom_dict, str):
                geom_dict = json.loads(geom_dict)
            
            # Validate geometry if requested
            if self.validate_geometry:
                try:
                    geom = shape(geom_dict)
                    if not geom.is_valid:
                        logger.warning("Invalid geometry detected")
                        # Try to fix the geometry
                        geom = geom.buffer(0)
                        if not geom.is_valid:
                            return None
                        geom_dict = shapely.geometry.mapping(geom)
                except Exception as e:
                    logger.warning(f"Geometry validation failed: {e}")
                    if self.validate_geometry:
                        return None
            
            return geom_dict
            
        except Exception as e:
            logger.warning(f"Failed to extract geometry: {e}")
            return None
    
    def _extract_properties(self, feature) -> Dict[str, Any]:
        """Extract properties from FlatGeobuf feature.
        
        Args:
            feature: FlatGeobuf feature object
            
        Returns:
            Properties dictionary
        """
        properties = {}
        
        try:
            # Get properties from feature
            if hasattr(feature, 'properties') and feature.properties():
                props = feature.properties()
                
                # Iterate through property columns
                for prop_name, prop_type in self._schema['properties'].items():
                    try:
                        value = getattr(props, prop_name, None)
                        
                        # Handle different property types
                        if value is not None:
                            if prop_type == 'string' and hasattr(value, 'decode'):
                                value = value.decode(self.encoding)
                            elif prop_type in ['byte', 'ubyte', 'short', 'ushort', 'int', 'uint']:
                                value = int(value)
                            elif prop_type in ['long', 'ulong']:
                                value = int(value)
                            elif prop_type in ['float', 'double']:
                                value = float(value)
                            elif prop_type == 'bool':
                                value = bool(value)
                        
                        properties[prop_name] = value
                        
                    except Exception as e:
                        logger.warning(f"Failed to extract property {prop_name}: {e}")
                        properties[prop_name] = None
                        
        except Exception as e:
            logger.warning(f"Failed to extract properties: {e}")
        
        return properties
    
    def get_schema(self) -> Dict[str, Any]:
        """Return schema information.
        
        Returns:
            Dictionary containing geometry type, properties schema, and CRS info
        """
        return self._schema.copy() if self._schema else {}
    
    def get_bounds(self) -> Tuple[float, float, float, float]:
        """Return spatial bounds.
        
        Returns:
            Tuple of (minx, miny, maxx, maxy)
        """
        return self._bounds or (0.0, 0.0, 0.0, 0.0)
    
    def get_feature_count(self) -> Optional[int]:
        """Return total number of features if available.
        
        Returns:
            Number of features or None if not available
        """
        return self._feature_count


class ReadFlatGeobufFn(DoFn):
    """Apache Beam DoFn for reading FlatGeobuf files."""
    
    def __init__(self, **kwargs):
        """Initialize the DoFn with options.
        
        Args:
            **kwargs: Options to pass to FlatGeobufReader
        """
        self.options = kwargs
    
    def process(self, file_path: str):
        """Process a single FlatGeobuf file.
        
        Args:
            file_path: Path to the FlatGeobuf file
            
        Yields:
            Feature dictionaries
        """
        try:
            reader = FlatGeobufReader(file_path, **self.options)
            
            for feature in reader.read_features():
                yield feature
                
        except Exception as e:
            logger.error(f"Failed to process file {file_path}: {e}")
            # Re-raise to fail the pipeline if needed
            raise


class SplittableFlatGeobufFn(DoFn):
    """Splittable DoFn for parallel reading of large FlatGeobuf files."""
    
    def __init__(self, **kwargs):
        """Initialize the splittable DoFn with options.
        
        Args:
            **kwargs: Options to pass to FlatGeobufReader
        """
        self.options = kwargs
    
    def process(self, file_path: str, restriction_tracker=DoFn.RestrictionParam):
        """Process a portion of a FlatGeobuf file.
        
        Args:
            file_path: Path to the FlatGeobuf file
            restriction_tracker: Beam restriction tracker for splitting
            
        Yields:
            Feature dictionaries
        """
        try:
            reader = FlatGeobufReader(file_path, **self.options)
            feature_count = reader.get_feature_count()
            
            if not feature_count:
                # If we can't get feature count, process normally
                for feature in reader.read_features():
                    yield feature
                return
            
            # Process features within the restriction range
            current_index = 0
            for feature in reader.read_features():
                if not restriction_tracker.try_claim(current_index):
                    break
                
                yield feature
                current_index += 1
                
        except Exception as e:
            logger.error(f"Failed to process file {file_path}: {e}")
            raise
    
    def create_initial_restriction(self, file_path: str) -> OffsetRange:
        """Create initial restriction for the file.
        
        Args:
            file_path: Path to the FlatGeobuf file
            
        Returns:
            OffsetRange covering all features in the file
        """
        try:
            reader = FlatGeobufReader(file_path, **self.options)
            feature_count = reader.get_feature_count()
            
            if feature_count and feature_count > 0:
                return OffsetRange(0, feature_count)
            else:
                # Default to single feature if count is unknown
                return OffsetRange(0, 1)
                
        except Exception as e:
            logger.warning(f"Failed to get feature count for {file_path}: {e}")
            return OffsetRange(0, 1)
    
    def split(self, file_path: str, restriction: OffsetRange) -> List[OffsetRange]:
        """Split the restriction into smaller chunks.
        
        Args:
            file_path: Path to the FlatGeobuf file
            restriction: Current restriction range
            
        Returns:
            List of smaller restriction ranges
        """
        # Split into chunks of approximately 1000 features each
        chunk_size = 1000
        splits = []
        
        start = restriction.start
        end = restriction.stop
        
        while start < end:
            chunk_end = min(start + chunk_size, end)
            splits.append(OffsetRange(start, chunk_end))
            start = chunk_end
        
        return splits if len(splits) > 1 else [restriction]
    
    def restriction_size(self, file_path: str, restriction: OffsetRange) -> int:
        """Get the size of the restriction.
        
        Args:
            file_path: Path to the FlatGeobuf file
            restriction: Restriction range
            
        Returns:
            Size of the restriction
        """
        return restriction.stop - restriction.start


class FlatGeobufTransform(BaseFormatTransform):
    """Apache Beam transform for reading FlatGeobuf files."""
    
    def __init__(self, file_pattern: str, use_splittable: bool = True, **kwargs):
        """Initialize FlatGeobuf transform.
        
        Args:
            file_pattern: File pattern to match (supports wildcards)
            use_splittable: Whether to use splittable DoFn for large files
            **kwargs: Additional format-specific parameters
        """
        super().__init__(file_pattern, **kwargs)
        self.use_splittable = use_splittable
    
    def expand(self, pcoll):
        """Expand the transform to read FlatGeobuf files.
        
        Args:
            pcoll: Input PCollection (typically empty)
            
        Returns:
            PCollection of feature dictionaries
        """
        if self.use_splittable:
            return (
                pcoll
                | 'Match Files' >> fileio.MatchFiles(self.file_pattern)
                | 'Read Matches' >> fileio.ReadMatches()
                | 'Extract File Paths' >> beam.Map(lambda x: x.metadata.path)
                | 'Read FlatGeobuf Splittable' >> beam.ParDo(SplittableFlatGeobufFn(**self.options))
                | 'Reshuffle' >> Reshuffle()  # For better parallelization
            )
        else:
            return (
                pcoll
                | 'Match Files' >> fileio.MatchFiles(self.file_pattern)
                | 'Read Matches' >> fileio.ReadMatches()
                | 'Extract File Paths' >> beam.Map(lambda x: x.metadata.path)
                | 'Read FlatGeobuf' >> beam.ParDo(ReadFlatGeobufFn(**self.options))
                | 'Reshuffle' >> Reshuffle()  # For better parallelization
            )

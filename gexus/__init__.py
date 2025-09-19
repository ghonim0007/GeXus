"""
GeXus: Open-source framework for big geospatial data processing.

GeXus bridges the gap between traditional GIS data formats and modern big data 
processing pipelines, enabling scalable geospatial data processing using Apache Beam.
"""

__version__ = "0.1.0"
__author__ = "ghonim0007"
__email__ = "ghonem717@gmail.com"

from .core import GeoProcessor
from .formats import (
    FlatGeobufReader,
    FlatGeobufTransform,
    BaseFormatReader,
    BaseFormatTransform
)
from .transforms import (
    SpatialFilterTransform,
    GeometryValidationTransform,
    CRSTransformTransform,
    BoundsCalculationTransform
)

__all__ = [
    "GeoProcessor",
    # Format readers
    "FlatGeobufReader",
    "FlatGeobufTransform", 
    "BaseFormatReader",
    "BaseFormatTransform",
    # Spatial transforms
    "SpatialFilterTransform",
    "GeometryValidationTransform",
    "CRSTransformTransform",
    "BoundsCalculationTransform"
]
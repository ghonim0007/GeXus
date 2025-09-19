"""
GeXus: Open-source framework for big geospatial data processing.

GeXus bridges the gap between traditional GIS data formats and modern big data 
processing pipelines, enabling scalable geospatial data processing using Apache Beam.
Supports both vector (FlatGeobuf) and raster (GeoTIFF) data processing at scale.
"""

__version__ = "0.2.0"
__author__ = "ghonim0007"
__email__ = "ghonem717@gmail.com"

from .core import GeoProcessor

# Vector format readers
from .formats import (
    FlatGeobufReader,
    FlatGeobufTransform,
    BaseFormatReader,
    BaseFormatTransform
)

# Raster format readers
from .formats.geotiff import (
    GeoTIFFReader,
    GeoTIFFTransform
)
from .formats.raster_base import (
    BaseRasterReader,
    BaseRasterTransform,
    RasterTile,
    RasterBand
)

# Vector transforms
from .transforms import (
    SpatialFilterTransform,
    GeometryValidationTransform,
    CRSTransformTransform,
    BoundsCalculationTransform
)

# Raster transforms
from .transforms.raster import (
    RasterBandMathTransform,
    RasterResampleTransform,
    ZonalStatisticsTransform,
    RasterFilterTransform,
    RasterMosaicTransform
)

# Analytics
from .analytics import (
    VegetationIndices,
    WaterIndices,
    UrbanIndices,
    SoilIndices,
    TemporalRasterAnalysis,
    ChangeDetectionAnalysis,
    TrendAnalysis
)

__all__ = [
    "GeoProcessor",
    
    # Vector format readers
    "FlatGeobufReader",
    "FlatGeobufTransform", 
    "BaseFormatReader",
    "BaseFormatTransform",
    
    # Raster format readers
    "GeoTIFFReader",
    "GeoTIFFTransform",
    "BaseRasterReader",
    "BaseRasterTransform",
    "RasterTile",
    "RasterBand",
    
    # Vector transforms
    "SpatialFilterTransform",
    "GeometryValidationTransform",
    "CRSTransformTransform",
    "BoundsCalculationTransform",
    
    # Raster transforms
    "RasterBandMathTransform",
    "RasterResampleTransform",
    "ZonalStatisticsTransform",
    "RasterFilterTransform",
    "RasterMosaicTransform",
    
    # Analytics
    "VegetationIndices",
    "WaterIndices",
    "UrbanIndices",
    "SoilIndices",
    "TemporalRasterAnalysis",
    "ChangeDetectionAnalysis",
    "TrendAnalysis"
]
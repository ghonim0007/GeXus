"""Base interface for geospatial format handlers."""

from abc import ABC, abstractmethod
from typing import Dict, Iterator, Tuple, Any, Optional
import apache_beam as beam


class BaseFormatReader(ABC):
    """Abstract base class for geospatial format readers."""
    
    def __init__(self, file_path: str, **kwargs):
        """Initialize reader with file path and optional parameters.
        
        Args:
            file_path: Path to the geospatial data file
            **kwargs: Additional format-specific parameters
        """
        self.file_path = file_path
        self.options = kwargs
    
    @abstractmethod
    def read_features(self) -> Iterator[Dict[str, Any]]:
        """Yield features one by one for memory efficiency.
        
        Returns:
            Iterator of feature dictionaries with 'geometry' and 'properties' keys
        """
        pass
    
    @abstractmethod
    def get_schema(self) -> Dict[str, Any]:
        """Return schema information.
        
        Returns:
            Dictionary containing geometry type, properties schema, and CRS info
        """
        pass
    
    @abstractmethod
    def get_bounds(self) -> Tuple[float, float, float, float]:
        """Return spatial bounds.
        
        Returns:
            Tuple of (minx, miny, maxx, maxy)
        """
        pass
    
    @abstractmethod
    def get_feature_count(self) -> Optional[int]:
        """Return total number of features if available.
        
        Returns:
            Number of features or None if not available
        """
        pass


class BaseFormatTransform(beam.PTransform, ABC):
    """Abstract base class for Apache Beam format transforms."""
    
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
        """Expand the transform to read files.
        
        Args:
            pcoll: Input PCollection (typically empty)
            
        Returns:
            PCollection of feature dictionaries
        """
        pass

"""Spectral indices calculations for remote sensing analysis."""

import logging
from typing import Dict, Any, Optional, Tuple
import numpy as np

logger = logging.getLogger(__name__)


class VegetationIndices:
    """Collection of vegetation indices for remote sensing analysis."""
    
    @staticmethod
    def ndvi(red_band: np.ndarray, nir_band: np.ndarray, 
             nodata: Optional[float] = None) -> np.ndarray:
        """Calculate Normalized Difference Vegetation Index (NDVI).
        
        NDVI = (NIR - Red) / (NIR + Red)
        
        Args:
            red_band: Red band array (typically band 4 in Landsat)
            nir_band: Near-infrared band array (typically band 5 in Landsat)
            nodata: Nodata value to mask
            
        Returns:
            NDVI array with values between -1 and 1
        """
        try:
            # Convert to float to avoid integer overflow
            red = red_band.astype(np.float32)
            nir = nir_band.astype(np.float32)
            
            # Mask nodata values
            if nodata is not None:
                red = np.where(red == nodata, np.nan, red)
                nir = np.where(nir == nodata, np.nan, nir)
            
            # Calculate NDVI
            denominator = nir + red
            ndvi = np.where(
                denominator != 0,
                (nir - red) / denominator,
                np.nan
            )
            
            # Clip to valid range
            ndvi = np.clip(ndvi, -1, 1)
            
            return ndvi
            
        except Exception as e:
            logger.error(f"NDVI calculation failed: {e}")
            raise
    
    @staticmethod
    def evi(blue_band: np.ndarray, red_band: np.ndarray, nir_band: np.ndarray,
            nodata: Optional[float] = None, 
            G: float = 2.5, C1: float = 6.0, C2: float = 7.5, L: float = 1.0) -> np.ndarray:
        """Calculate Enhanced Vegetation Index (EVI).
        
        EVI = G * ((NIR - Red) / (NIR + C1*Red - C2*Blue + L))
        
        Args:
            blue_band: Blue band array
            red_band: Red band array
            nir_band: Near-infrared band array
            nodata: Nodata value to mask
            G: Gain factor (default: 2.5)
            C1: Coefficient for red band (default: 6.0)
            C2: Coefficient for blue band (default: 7.5)
            L: Canopy background adjustment (default: 1.0)
            
        Returns:
            EVI array
        """
        try:
            # Convert to float
            blue = blue_band.astype(np.float32)
            red = red_band.astype(np.float32)
            nir = nir_band.astype(np.float32)
            
            # Mask nodata values
            if nodata is not None:
                blue = np.where(blue == nodata, np.nan, blue)
                red = np.where(red == nodata, np.nan, red)
                nir = np.where(nir == nodata, np.nan, nir)
            
            # Calculate EVI
            denominator = nir + C1 * red - C2 * blue + L
            evi = np.where(
                denominator != 0,
                G * (nir - red) / denominator,
                np.nan
            )
            
            return evi
            
        except Exception as e:
            logger.error(f"EVI calculation failed: {e}")
            raise
    
    @staticmethod
    def savi(red_band: np.ndarray, nir_band: np.ndarray,
             L: float = 0.5, nodata: Optional[float] = None) -> np.ndarray:
        """Calculate Soil-Adjusted Vegetation Index (SAVI).
        
        SAVI = ((NIR - Red) / (NIR + Red + L)) * (1 + L)
        
        Args:
            red_band: Red band array
            nir_band: Near-infrared band array
            L: Soil brightness correction factor (default: 0.5)
            nodata: Nodata value to mask
            
        Returns:
            SAVI array
        """
        try:
            # Convert to float
            red = red_band.astype(np.float32)
            nir = nir_band.astype(np.float32)
            
            # Mask nodata values
            if nodata is not None:
                red = np.where(red == nodata, np.nan, red)
                nir = np.where(nir == nodata, np.nan, nir)
            
            # Calculate SAVI
            denominator = nir + red + L
            savi = np.where(
                denominator != 0,
                ((nir - red) / denominator) * (1 + L),
                np.nan
            )
            
            return savi
            
        except Exception as e:
            logger.error(f"SAVI calculation failed: {e}")
            raise
    
    @staticmethod
    def gndvi(green_band: np.ndarray, nir_band: np.ndarray,
              nodata: Optional[float] = None) -> np.ndarray:
        """Calculate Green Normalized Difference Vegetation Index (GNDVI).
        
        GNDVI = (NIR - Green) / (NIR + Green)
        
        Args:
            green_band: Green band array
            nir_band: Near-infrared band array
            nodata: Nodata value to mask
            
        Returns:
            GNDVI array
        """
        try:
            # Convert to float
            green = green_band.astype(np.float32)
            nir = nir_band.astype(np.float32)
            
            # Mask nodata values
            if nodata is not None:
                green = np.where(green == nodata, np.nan, green)
                nir = np.where(nir == nodata, np.nan, nir)
            
            # Calculate GNDVI
            denominator = nir + green
            gndvi = np.where(
                denominator != 0,
                (nir - green) / denominator,
                np.nan
            )
            
            return gndvi
            
        except Exception as e:
            logger.error(f"GNDVI calculation failed: {e}")
            raise


class WaterIndices:
    """Collection of water indices for remote sensing analysis."""
    
    @staticmethod
    def ndwi(green_band: np.ndarray, nir_band: np.ndarray,
             nodata: Optional[float] = None) -> np.ndarray:
        """Calculate Normalized Difference Water Index (NDWI).
        
        NDWI = (Green - NIR) / (Green + NIR)
        
        Args:
            green_band: Green band array
            nir_band: Near-infrared band array
            nodata: Nodata value to mask
            
        Returns:
            NDWI array
        """
        try:
            # Convert to float
            green = green_band.astype(np.float32)
            nir = nir_band.astype(np.float32)
            
            # Mask nodata values
            if nodata is not None:
                green = np.where(green == nodata, np.nan, green)
                nir = np.where(nir == nodata, np.nan, nir)
            
            # Calculate NDWI
            denominator = green + nir
            ndwi = np.where(
                denominator != 0,
                (green - nir) / denominator,
                np.nan
            )
            
            return ndwi
            
        except Exception as e:
            logger.error(f"NDWI calculation failed: {e}")
            raise
    
    @staticmethod
    def mndwi(green_band: np.ndarray, swir_band: np.ndarray,
              nodata: Optional[float] = None) -> np.ndarray:
        """Calculate Modified Normalized Difference Water Index (MNDWI).
        
        MNDWI = (Green - SWIR) / (Green + SWIR)
        
        Args:
            green_band: Green band array
            swir_band: Short-wave infrared band array
            nodata: Nodata value to mask
            
        Returns:
            MNDWI array
        """
        try:
            # Convert to float
            green = green_band.astype(np.float32)
            swir = swir_band.astype(np.float32)
            
            # Mask nodata values
            if nodata is not None:
                green = np.where(green == nodata, np.nan, green)
                swir = np.where(swir == nodata, np.nan, swir)
            
            # Calculate MNDWI
            denominator = green + swir
            mndwi = np.where(
                denominator != 0,
                (green - swir) / denominator,
                np.nan
            )
            
            return mndwi
            
        except Exception as e:
            logger.error(f"MNDWI calculation failed: {e}")
            raise


class UrbanIndices:
    """Collection of urban indices for remote sensing analysis."""
    
    @staticmethod
    def ndbi(swir_band: np.ndarray, nir_band: np.ndarray,
             nodata: Optional[float] = None) -> np.ndarray:
        """Calculate Normalized Difference Built-up Index (NDBI).
        
        NDBI = (SWIR - NIR) / (SWIR + NIR)
        
        Args:
            swir_band: Short-wave infrared band array
            nir_band: Near-infrared band array
            nodata: Nodata value to mask
            
        Returns:
            NDBI array
        """
        try:
            # Convert to float
            swir = swir_band.astype(np.float32)
            nir = nir_band.astype(np.float32)
            
            # Mask nodata values
            if nodata is not None:
                swir = np.where(swir == nodata, np.nan, swir)
                nir = np.where(nir == nodata, np.nan, nir)
            
            # Calculate NDBI
            denominator = swir + nir
            ndbi = np.where(
                denominator != 0,
                (swir - nir) / denominator,
                np.nan
            )
            
            return ndbi
            
        except Exception as e:
            logger.error(f"NDBI calculation failed: {e}")
            raise
    
    @staticmethod
    def ui(red_band: np.ndarray, nir_band: np.ndarray,
           nodata: Optional[float] = None) -> np.ndarray:
        """Calculate Urban Index (UI).
        
        UI = (NIR - Red) / (NIR + Red)
        Note: This is similar to NDVI but interpreted for urban areas
        
        Args:
            red_band: Red band array
            nir_band: Near-infrared band array
            nodata: Nodata value to mask
            
        Returns:
            UI array
        """
        try:
            # This is essentially NDVI but interpreted differently
            return VegetationIndices.ndvi(red_band, nir_band, nodata)
            
        except Exception as e:
            logger.error(f"UI calculation failed: {e}")
            raise


class SoilIndices:
    """Collection of soil indices for remote sensing analysis."""
    
    @staticmethod
    def bsi(blue_band: np.ndarray, red_band: np.ndarray, 
            nir_band: np.ndarray, swir_band: np.ndarray,
            nodata: Optional[float] = None) -> np.ndarray:
        """Calculate Bare Soil Index (BSI).
        
        BSI = ((SWIR + Red) - (NIR + Blue)) / ((SWIR + Red) + (NIR + Blue))
        
        Args:
            blue_band: Blue band array
            red_band: Red band array
            nir_band: Near-infrared band array
            swir_band: Short-wave infrared band array
            nodata: Nodata value to mask
            
        Returns:
            BSI array
        """
        try:
            # Convert to float
            blue = blue_band.astype(np.float32)
            red = red_band.astype(np.float32)
            nir = nir_band.astype(np.float32)
            swir = swir_band.astype(np.float32)
            
            # Mask nodata values
            if nodata is not None:
                blue = np.where(blue == nodata, np.nan, blue)
                red = np.where(red == nodata, np.nan, red)
                nir = np.where(nir == nodata, np.nan, nir)
                swir = np.where(swir == nodata, np.nan, swir)
            
            # Calculate BSI
            numerator = (swir + red) - (nir + blue)
            denominator = (swir + red) + (nir + blue)
            
            bsi = np.where(
                denominator != 0,
                numerator / denominator,
                np.nan
            )
            
            return bsi
            
        except Exception as e:
            logger.error(f"BSI calculation failed: {e}")
            raise
    
    @staticmethod
    def si(red_band: np.ndarray, nir_band: np.ndarray,
           nodata: Optional[float] = None) -> np.ndarray:
        """Calculate Soil Index (SI).
        
        SI = (Red - NIR) / (Red + NIR)
        
        Args:
            red_band: Red band array
            nir_band: Near-infrared band array
            nodata: Nodata value to mask
            
        Returns:
            SI array
        """
        try:
            # Convert to float
            red = red_band.astype(np.float32)
            nir = nir_band.astype(np.float32)
            
            # Mask nodata values
            if nodata is not None:
                red = np.where(red == nodata, np.nan, red)
                nir = np.where(nir == nodata, np.nan, nir)
            
            # Calculate SI
            denominator = red + nir
            si = np.where(
                denominator != 0,
                (red - nir) / denominator,
                np.nan
            )
            
            return si
            
        except Exception as e:
            logger.error(f"SI calculation failed: {e}")
            raise


class IndexCalculator:
    """Unified interface for calculating various spectral indices."""
    
    # Index registry
    INDICES = {
        # Vegetation indices
        'ndvi': VegetationIndices.ndvi,
        'evi': VegetationIndices.evi,
        'savi': VegetationIndices.savi,
        'gndvi': VegetationIndices.gndvi,
        
        # Water indices
        'ndwi': WaterIndices.ndwi,
        'mndwi': WaterIndices.mndwi,
        
        # Urban indices
        'ndbi': UrbanIndices.ndbi,
        'ui': UrbanIndices.ui,
        
        # Soil indices
        'bsi': SoilIndices.bsi,
        'si': SoilIndices.si
    }
    
    @classmethod
    def calculate(cls, index_name: str, bands: Dict[str, np.ndarray], 
                  **kwargs) -> np.ndarray:
        """Calculate a spectral index by name.
        
        Args:
            index_name: Name of the index to calculate
            bands: Dictionary of band arrays with keys like 'red', 'nir', etc.
            **kwargs: Additional parameters for the index calculation
            
        Returns:
            Calculated index array
            
        Raises:
            ValueError: If index name is not supported
            KeyError: If required bands are missing
        """
        if index_name.lower() not in cls.INDICES:
            raise ValueError(f"Unsupported index: {index_name}. "
                           f"Supported indices: {list(cls.INDICES.keys())}")
        
        index_func = cls.INDICES[index_name.lower()]
        
        try:
            # Map common band names to function parameters
            if index_name.lower() == 'ndvi':
                return index_func(bands['red'], bands['nir'], **kwargs)
            elif index_name.lower() == 'evi':
                return index_func(bands['blue'], bands['red'], bands['nir'], **kwargs)
            elif index_name.lower() == 'savi':
                return index_func(bands['red'], bands['nir'], **kwargs)
            elif index_name.lower() == 'gndvi':
                return index_func(bands['green'], bands['nir'], **kwargs)
            elif index_name.lower() == 'ndwi':
                return index_func(bands['green'], bands['nir'], **kwargs)
            elif index_name.lower() == 'mndwi':
                return index_func(bands['green'], bands['swir'], **kwargs)
            elif index_name.lower() == 'ndbi':
                return index_func(bands['swir'], bands['nir'], **kwargs)
            elif index_name.lower() == 'ui':
                return index_func(bands['red'], bands['nir'], **kwargs)
            elif index_name.lower() == 'bsi':
                return index_func(bands['blue'], bands['red'], 
                                bands['nir'], bands['swir'], **kwargs)
            elif index_name.lower() == 'si':
                return index_func(bands['red'], bands['nir'], **kwargs)
            else:
                raise ValueError(f"Index calculation not implemented: {index_name}")
                
        except KeyError as e:
            raise KeyError(f"Missing required band for {index_name}: {e}")
        except Exception as e:
            logger.error(f"Failed to calculate {index_name}: {e}")
            raise
    
    @classmethod
    def get_required_bands(cls, index_name: str) -> List[str]:
        """Get list of required bands for an index.
        
        Args:
            index_name: Name of the index
            
        Returns:
            List of required band names
        """
        band_requirements = {
            'ndvi': ['red', 'nir'],
            'evi': ['blue', 'red', 'nir'],
            'savi': ['red', 'nir'],
            'gndvi': ['green', 'nir'],
            'ndwi': ['green', 'nir'],
            'mndwi': ['green', 'swir'],
            'ndbi': ['swir', 'nir'],
            'ui': ['red', 'nir'],
            'bsi': ['blue', 'red', 'nir', 'swir'],
            'si': ['red', 'nir']
        }
        
        return band_requirements.get(index_name.lower(), [])

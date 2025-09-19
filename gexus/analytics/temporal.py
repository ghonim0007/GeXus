"""Temporal analysis functions for raster time series data."""

import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import numpy as np

logger = logging.getLogger(__name__)


class TemporalRasterAnalysis:
    """Analyze temporal changes in raster data."""
    
    def __init__(self, raster_time_series: List[Tuple[str, np.ndarray, datetime]]):
        """Initialize temporal analysis.
        
        Args:
            raster_time_series: List of tuples (file_path, data_array, timestamp)
        """
        self.time_series = sorted(raster_time_series, key=lambda x: x[2])
        self.timestamps = [item[2] for item in self.time_series]
        self.data_arrays = [item[1] for item in self.time_series]
        
        if len(self.time_series) < 2:
            raise ValueError("At least 2 time points required for temporal analysis")
    
    def calculate_change_detection(self, method: str = 'difference',
                                 threshold: Optional[float] = None) -> np.ndarray:
        """Detect changes between time periods.
        
        Args:
            method: Change detection method ('difference', 'ratio', 'normalized_difference')
            threshold: Threshold for significant change detection
            
        Returns:
            Change detection array
        """
        try:
            if len(self.data_arrays) < 2:
                raise ValueError("Need at least 2 time points for change detection")
            
            before = self.data_arrays[0].astype(np.float32)
            after = self.data_arrays[-1].astype(np.float32)
            
            if method == 'difference':
                change = after - before
            elif method == 'ratio':
                change = np.where(before != 0, after / before, np.nan)
            elif method == 'normalized_difference':
                denominator = after + before
                change = np.where(
                    denominator != 0,
                    (after - before) / denominator,
                    np.nan
                )
            else:
                raise ValueError(f"Unsupported change detection method: {method}")
            
            # Apply threshold if specified
            if threshold is not None:
                if method == 'difference' or method == 'normalized_difference':
                    change = np.where(np.abs(change) > threshold, change, 0)
                elif method == 'ratio':
                    change = np.where(
                        (change > (1 + threshold)) | (change < (1 - threshold)),
                        change, 1
                    )
            
            return change
            
        except Exception as e:
            logger.error(f"Change detection failed: {e}")
            raise
    
    def trend_analysis(self, method: str = 'linear') -> Dict[str, np.ndarray]:
        """Calculate temporal trends.
        
        Args:
            method: Trend analysis method ('linear', 'mann_kendall')
            
        Returns:
            Dictionary with trend statistics
        """
        try:
            if len(self.data_arrays) < 3:
                raise ValueError("Need at least 3 time points for trend analysis")
            
            # Convert timestamps to numeric values (days since first observation)
            time_numeric = np.array([
                (ts - self.timestamps[0]).days for ts in self.timestamps
            ])
            
            # Stack all arrays
            data_stack = np.stack(self.data_arrays, axis=0)
            
            if method == 'linear':
                return self._linear_trend(data_stack, time_numeric)
            elif method == 'mann_kendall':
                return self._mann_kendall_trend(data_stack)
            else:
                raise ValueError(f"Unsupported trend method: {method}")
                
        except Exception as e:
            logger.error(f"Trend analysis failed: {e}")
            raise
    
    def _linear_trend(self, data_stack: np.ndarray, 
                     time_numeric: np.ndarray) -> Dict[str, np.ndarray]:
        """Calculate linear trend using least squares regression."""
        n_times, height, width = data_stack.shape
        
        # Initialize output arrays
        slope = np.full((height, width), np.nan, dtype=np.float32)
        intercept = np.full((height, width), np.nan, dtype=np.float32)
        r_squared = np.full((height, width), np.nan, dtype=np.float32)
        p_value = np.full((height, width), np.nan, dtype=np.float32)
        
        # Calculate means
        x_mean = np.mean(time_numeric)
        
        for i in range(height):
            for j in range(width):
                pixel_values = data_stack[:, i, j]
                
                # Skip if any values are NaN
                valid_mask = ~np.isnan(pixel_values)
                if np.sum(valid_mask) < 3:  # Need at least 3 points
                    continue
                
                x_valid = time_numeric[valid_mask]
                y_valid = pixel_values[valid_mask]
                
                if len(x_valid) < 3:
                    continue
                
                # Calculate linear regression
                y_mean = np.mean(y_valid)
                
                # Slope calculation
                numerator = np.sum((x_valid - x_mean) * (y_valid - y_mean))
                denominator = np.sum((x_valid - x_mean) ** 2)
                
                if denominator != 0:
                    slope[i, j] = numerator / denominator
                    intercept[i, j] = y_mean - slope[i, j] * x_mean
                    
                    # R-squared calculation
                    y_pred = slope[i, j] * x_valid + intercept[i, j]
                    ss_res = np.sum((y_valid - y_pred) ** 2)
                    ss_tot = np.sum((y_valid - y_mean) ** 2)
                    
                    if ss_tot != 0:
                        r_squared[i, j] = 1 - (ss_res / ss_tot)
                    
                    # Simple p-value approximation (t-test)
                    if len(x_valid) > 2:
                        se_slope = np.sqrt(ss_res / (len(x_valid) - 2)) / np.sqrt(denominator)
                        if se_slope != 0:
                            t_stat = slope[i, j] / se_slope
                            # Simplified p-value (assuming normal distribution)
                            p_value[i, j] = 2 * (1 - abs(t_stat) / (abs(t_stat) + 1))
        
        return {
            'slope': slope,
            'intercept': intercept,
            'r_squared': r_squared,
            'p_value': p_value
        }
    
    def _mann_kendall_trend(self, data_stack: np.ndarray) -> Dict[str, np.ndarray]:
        """Calculate Mann-Kendall trend test."""
        n_times, height, width = data_stack.shape
        
        # Initialize output arrays
        tau = np.full((height, width), np.nan, dtype=np.float32)
        p_value = np.full((height, width), np.nan, dtype=np.float32)
        trend = np.full((height, width), 0, dtype=np.int8)  # -1, 0, 1 for decreasing, no trend, increasing
        
        for i in range(height):
            for j in range(width):
                pixel_values = data_stack[:, i, j]
                
                # Skip if any values are NaN
                valid_mask = ~np.isnan(pixel_values)
                if np.sum(valid_mask) < 3:
                    continue
                
                y_valid = pixel_values[valid_mask]
                n = len(y_valid)
                
                if n < 3:
                    continue
                
                # Calculate Mann-Kendall statistic
                s = 0
                for k in range(n - 1):
                    for l in range(k + 1, n):
                        if y_valid[l] > y_valid[k]:
                            s += 1
                        elif y_valid[l] < y_valid[k]:
                            s -= 1
                
                # Calculate variance
                var_s = n * (n - 1) * (2 * n + 5) / 18
                
                # Calculate standardized test statistic
                if s > 0:
                    z = (s - 1) / np.sqrt(var_s)
                    trend[i, j] = 1
                elif s < 0:
                    z = (s + 1) / np.sqrt(var_s)
                    trend[i, j] = -1
                else:
                    z = 0
                    trend[i, j] = 0
                
                # Calculate Kendall's tau
                tau[i, j] = s / (n * (n - 1) / 2)
                
                # Approximate p-value (two-tailed)
                p_value[i, j] = 2 * (1 - abs(z) / (abs(z) + 1))
        
        return {
            'tau': tau,
            'p_value': p_value,
            'trend': trend,
            'trend_direction': trend
        }
    
    def calculate_statistics(self) -> Dict[str, np.ndarray]:
        """Calculate basic temporal statistics.
        
        Returns:
            Dictionary with temporal statistics
        """
        try:
            data_stack = np.stack(self.data_arrays, axis=0)
            
            return {
                'mean': np.nanmean(data_stack, axis=0),
                'std': np.nanstd(data_stack, axis=0),
                'min': np.nanmin(data_stack, axis=0),
                'max': np.nanmax(data_stack, axis=0),
                'range': np.nanmax(data_stack, axis=0) - np.nanmin(data_stack, axis=0),
                'cv': np.nanstd(data_stack, axis=0) / np.nanmean(data_stack, axis=0)
            }
            
        except Exception as e:
            logger.error(f"Statistics calculation failed: {e}")
            raise


class ChangeDetectionAnalysis:
    """Specialized change detection analysis."""
    
    @staticmethod
    def binary_change_detection(before: np.ndarray, after: np.ndarray,
                               threshold: float, method: str = 'difference') -> np.ndarray:
        """Perform binary change detection.
        
        Args:
            before: Before image array
            after: After image array
            threshold: Change threshold
            method: Detection method ('difference', 'ratio', 'chi_square')
            
        Returns:
            Binary change mask (1 = change, 0 = no change)
        """
        try:
            before = before.astype(np.float32)
            after = after.astype(np.float32)
            
            if method == 'difference':
                change_magnitude = np.abs(after - before)
                change_mask = change_magnitude > threshold
            elif method == 'ratio':
                ratio = np.where(before != 0, after / before, 1)
                change_mask = (ratio > (1 + threshold)) | (ratio < (1 - threshold))
            elif method == 'chi_square':
                # Chi-square change detection
                expected = (before + after) / 2
                chi_square = np.where(
                    expected != 0,
                    ((before - expected) ** 2 + (after - expected) ** 2) / expected,
                    0
                )
                change_mask = chi_square > threshold
            else:
                raise ValueError(f"Unsupported method: {method}")
            
            return change_mask.astype(np.uint8)
            
        except Exception as e:
            logger.error(f"Binary change detection failed: {e}")
            raise
    
    @staticmethod
    def change_vector_analysis(before: np.ndarray, after: np.ndarray) -> Dict[str, np.ndarray]:
        """Perform change vector analysis for multi-band data.
        
        Args:
            before: Before image array (bands, height, width)
            after: After image array (bands, height, width)
            
        Returns:
            Dictionary with change magnitude and direction
        """
        try:
            before = before.astype(np.float32)
            after = after.astype(np.float32)
            
            # Calculate change vector
            change_vector = after - before
            
            # Calculate magnitude
            magnitude = np.sqrt(np.sum(change_vector ** 2, axis=0))
            
            # Calculate direction (angle) for first two bands
            if change_vector.shape[0] >= 2:
                direction = np.arctan2(change_vector[1], change_vector[0])
                direction_degrees = np.degrees(direction)
            else:
                direction_degrees = np.zeros_like(magnitude)
            
            return {
                'magnitude': magnitude,
                'direction': direction_degrees,
                'change_vector': change_vector
            }
            
        except Exception as e:
            logger.error(f"Change vector analysis failed: {e}")
            raise


class TrendAnalysis:
    """Advanced trend analysis for time series data."""
    
    @staticmethod
    def seasonal_decomposition(data: np.ndarray, timestamps: List[datetime],
                             period: int = 365) -> Dict[str, np.ndarray]:
        """Perform seasonal decomposition of time series.
        
        Args:
            data: Time series data (time, height, width)
            timestamps: List of timestamps
            period: Seasonal period in days
            
        Returns:
            Dictionary with trend, seasonal, and residual components
        """
        try:
            n_times, height, width = data.shape
            
            # Initialize output arrays
            trend = np.full_like(data, np.nan)
            seasonal = np.full_like(data, np.nan)
            residual = np.full_like(data, np.nan)
            
            # Convert timestamps to day of year
            day_of_year = np.array([ts.timetuple().tm_yday for ts in timestamps])
            
            # Simple moving average for trend (can be improved)
            window_size = min(period // 4, n_times // 4)
            if window_size < 3:
                window_size = 3
            
            for i in range(height):
                for j in range(width):
                    pixel_series = data[:, i, j]
                    
                    if np.all(np.isnan(pixel_series)):
                        continue
                    
                    # Calculate trend using moving average
                    trend_series = np.full(n_times, np.nan)
                    for t in range(n_times):
                        start_idx = max(0, t - window_size // 2)
                        end_idx = min(n_times, t + window_size // 2 + 1)
                        trend_series[t] = np.nanmean(pixel_series[start_idx:end_idx])
                    
                    trend[:, i, j] = trend_series
                    
                    # Calculate seasonal component
                    detrended = pixel_series - trend_series
                    seasonal_series = np.full(n_times, np.nan)
                    
                    for doy in range(1, 367):
                        mask = day_of_year == doy
                        if np.any(mask):
                            seasonal_series[mask] = np.nanmean(detrended[mask])
                    
                    seasonal[:, i, j] = seasonal_series
                    
                    # Calculate residual
                    residual[:, i, j] = pixel_series - trend_series - seasonal_series
            
            return {
                'trend': trend,
                'seasonal': seasonal,
                'residual': residual
            }
            
        except Exception as e:
            logger.error(f"Seasonal decomposition failed: {e}")
            raise
    
    @staticmethod
    def breakpoint_detection(data: np.ndarray, timestamps: List[datetime],
                           min_segment_length: int = 10) -> Dict[str, np.ndarray]:
        """Detect breakpoints in time series data.
        
        Args:
            data: Time series data (time, height, width)
            timestamps: List of timestamps
            min_segment_length: Minimum length of segments
            
        Returns:
            Dictionary with breakpoint locations and statistics
        """
        try:
            n_times, height, width = data.shape
            
            # Initialize output arrays
            breakpoints = np.full((height, width), -1, dtype=np.int32)
            breakpoint_magnitude = np.full((height, width), np.nan, dtype=np.float32)
            
            for i in range(height):
                for j in range(width):
                    pixel_series = data[:, i, j]
                    
                    if np.sum(~np.isnan(pixel_series)) < min_segment_length * 2:
                        continue
                    
                    # Simple breakpoint detection using CUSUM
                    max_cusum = 0
                    max_cusum_idx = -1
                    
                    mean_val = np.nanmean(pixel_series)
                    cusum_pos = 0
                    cusum_neg = 0
                    
                    for t in range(min_segment_length, n_times - min_segment_length):
                        if not np.isnan(pixel_series[t]):
                            deviation = pixel_series[t] - mean_val
                            cusum_pos = max(0, cusum_pos + deviation)
                            cusum_neg = min(0, cusum_neg + deviation)
                            
                            cusum_magnitude = max(abs(cusum_pos), abs(cusum_neg))
                            
                            if cusum_magnitude > max_cusum:
                                max_cusum = cusum_magnitude
                                max_cusum_idx = t
                    
                    if max_cusum_idx > 0:
                        breakpoints[i, j] = max_cusum_idx
                        breakpoint_magnitude[i, j] = max_cusum
            
            return {
                'breakpoints': breakpoints,
                'magnitude': breakpoint_magnitude
            }
            
        except Exception as e:
            logger.error(f"Breakpoint detection failed: {e}")
            raise

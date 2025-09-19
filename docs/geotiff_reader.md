# GeoTIFF Reader for GeXus Framework

## Overview

The GeoTIFF reader is a production-ready implementation for processing GeoTIFF raster files within the GeXus framework. It provides seamless integration with Apache Beam pipelines for scalable raster data processing, enabling analysis of satellite imagery, Digital Elevation Models (DEMs), and multi-band raster datasets at cloud scale.

## Features

- **Tile-based Processing**: Memory-efficient tile-based reading with configurable tile sizes
- **Apache Beam Integration**: Native support for Beam pipelines with parallel processing
- **Splittable DoFn**: Automatic splitting of large files for distributed processing
- **Multi-band Support**: Handle single-band and multi-band raster datasets
- **Cloud-Optimized GeoTIFF (COG)**: Optimized support for COG files
- **Spatial Filtering**: Built-in spatial filtering using bounding boxes
- **Band Math Operations**: Built-in support for spectral indices and band calculations
- **Vector-Raster Integration**: Seamless integration with vector data processing
- **Error Handling**: Robust error handling for corrupted or invalid files

## Quick Start

### Basic Usage

```python
from gexus.core import GeoProcessor
from gexus.formats.geotiff import GeoTIFFTransform

# Create processor and pipeline
processor = GeoProcessor()
pipeline = processor.create_pipeline()

# Read GeoTIFF data as tiles
tiles = (
    pipeline 
    | 'Read Raster' >> GeoTIFFTransform('path/to/data/*.tif')
    | 'Process Tiles' >> beam.Map(lambda x: process_tile(x))
)

# Run pipeline
result = pipeline.run()
```

### Direct Reader Usage

```python
from gexus.formats.geotiff import GeoTIFFReader

# Create reader
reader = GeoTIFFReader('satellite_image.tif')

# Get metadata
metadata = reader.get_metadata()
print(f"Dimensions: {metadata['width']} x {metadata['height']}")
print(f"Bands: {metadata['count']}")
print(f"Data type: {metadata['dtype']}")

# Get spatial information
bounds = reader.get_bounds()
print(f"Bounds: {bounds}")  # (left, bottom, right, top)

# Read tiles
for tile in reader.read_tiles(tile_size=(512, 512)):
    print(f"Tile {tile.tile_id}: {tile.data.shape}")
```

## Advanced Usage

### Band Selection and Spatial Filtering

```python
from gexus.formats.geotiff import GeoTIFFTransform

# Read specific bands with spatial filtering
bbox = (-122.5, 37.7, -122.3, 37.8)  # San Francisco area
tiles = (
    pipeline
    | 'Read Landsat' >> GeoTIFFTransform(
        'landsat8/*.tif',
        bands=[4, 5],  # Red and NIR bands
        bbox=bbox,
        tile_size=(1024, 1024)
    )
)
```

### Band Math and Spectral Indices

```python
from gexus.transforms.raster import RasterBandMathTransform
from gexus.analytics.indices import IndexCalculator

# Calculate NDVI using band math
ndvi_tiles = (
    pipeline
    | 'Read Satellite' >> GeoTIFFTransform('satellite.tif')
    | 'Calculate NDVI' >> RasterBandMathTransform(
        expression="(B4 - B3) / (B4 + B3)",
        output_name="ndvi"
    )
)

# Or use the indices calculator
def calculate_vegetation_indices(tile):
    bands = {
        'red': tile.data[0],
        'nir': tile.data[1]
    }
    ndvi = IndexCalculator.calculate('ndvi', bands)
    return tile._replace(data=ndvi[np.newaxis, ...])

ndvi_tiles = tiles | 'Calculate NDVI' >> beam.Map(calculate_vegetation_indices)
```

### Vector-Raster Integration

```python
from gexus.formats.flatgeobuf import FlatGeobufTransform
from gexus.transforms.raster import ZonalStatisticsTransform

# Combined vector-raster workflow
vector_boundaries = pipeline | 'Read Boundaries' >> FlatGeobufTransform('boundaries.fgb')
raster_tiles = pipeline | 'Read Raster' >> GeoTIFFTransform('land_cover.tif')

zonal_stats = raster_tiles | 'Calculate Zonal Stats' >> ZonalStatisticsTransform(
    vector_boundaries=vector_boundaries,
    stats=['mean', 'std', 'count', 'min', 'max']
)
```

### Temporal Analysis

```python
from gexus.analytics.temporal import TemporalRasterAnalysis, ChangeDetectionAnalysis

# Multi-temporal change detection
before_tiles = pipeline | 'Read Before' >> GeoTIFFTransform('before/*.tif')
after_tiles = pipeline | 'Read After' >> GeoTIFFTransform('after/*.tif')

def calculate_change(before_after):
    before_tile, after_tile = before_after
    change = ChangeDetectionAnalysis.binary_change_detection(
        before_tile.data, after_tile.data, threshold=100
    )
    return {'change_mask': change, 'location': before_tile.tile_id}

changes = (
    (before_tiles, after_tiles)
    | 'Pair Tiles' >> beam.Zip()
    | 'Detect Changes' >> beam.Map(calculate_change)
)
```

### Parallel Processing

```python
# Enable splittable processing for large files
tiles = (
    pipeline
    | 'Read Large Raster' >> GeoTIFFTransform(
        'very_large_file.tif',
        use_splittable=True,  # Default is True
        tile_size=(512, 512)
    )
)

# Disable splittable processing for small files
tiles = (
    pipeline
    | 'Read Small Raster' >> GeoTIFFTransform(
        'small_file.tif',
        use_splittable=False
    )
)
```

## Configuration Options

### GeoTIFFReader Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `bands` | `List[int]` | `None` | List of band numbers to read (1-indexed, None for all) |
| `bbox` | `Tuple[float, float, float, float]` | `None` | Spatial filter bounding box (left, bottom, right, top) |
| `overview_level` | `int` | `0` | Overview level to read (0 for full resolution) |
| `chunk_size` | `int` | `512` | Size for chunked reading |
| `validate_cog` | `bool` | `False` | Whether to validate Cloud-Optimized GeoTIFF structure |

### GeoTIFFTransform Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `bands` | `List[int]` | `None` | List of band numbers to read (1-indexed) |
| `tile_size` | `Tuple[int, int]` | `(512, 512)` | Size of tiles as (width, height) in pixels |
| `bbox` | `Tuple[float, ...]` | `None` | Bounding box for spatial filtering |
| `use_overviews` | `bool` | `True` | Whether to use overview levels for efficiency |
| `output_mode` | `str` | `'tiles'` | Output mode: 'tiles' or 'bands' |
| `use_splittable` | `bool` | `True` | Whether to use splittable DoFn for large files |

## Performance Tuning

### Memory Usage

The GeoTIFF reader is designed for efficient memory usage:

```python
# Memory-efficient tile processing
reader = GeoTIFFReader('large_file.tif')
for tile in reader.read_tiles(tile_size=(512, 512)):
    # Process one tile at a time - O(tile_size) memory usage
    process_tile(tile)
```

### Parallel Processing

For large files, enable splittable processing:

```python
# Automatic splitting for parallel processing
tiles = (
    pipeline
    | 'Read Large Raster' >> GeoTIFFTransform(
        'multi_gb_file.tif',
        use_splittable=True,
        tile_size=(1024, 1024)  # Larger tiles for better performance
    )
)
```

### Cloud-Optimized GeoTIFF (COG)

Leverage COG features for cloud processing:

```python
# Efficient cloud processing with COG
tiles = (
    pipeline
    | 'Read COG' >> GeoTIFFTransform(
        'gs://bucket/cog_files/*.tif',
        use_overviews=True,  # Use overviews for faster processing
        bbox=area_of_interest  # Only read required area
    )
)
```

## Error Handling

### File Errors

```python
try:
    reader = GeoTIFFReader('nonexistent.tif')
except FileNotFoundError as e:
    print(f"File not found: {e}")
```

### Data Errors

```python
# Handle invalid raster data
def safe_process_tile(tile):
    try:
        if np.all(np.isnan(tile.data)):
            return None  # Skip tiles with all NaN values
        return process_tile(tile)
    except Exception as e:
        logging.warning(f"Failed to process tile {tile.tile_id}: {e}")
        return None

processed_tiles = (
    tiles
    | 'Safe Process' >> beam.Map(safe_process_tile)
    | 'Filter None' >> beam.Filter(lambda x: x is not None)
)
```

### Pipeline Errors

```python
def robust_raster_processing(tile):
    try:
        # Apply band math
        result = calculate_ndvi(tile)
        return result
    except Exception as e:
        logging.error(f"Processing failed for tile {tile.tile_id}: {e}")
        # Return tile with error flag
        return tile._replace(tile_id=f"{tile.tile_id}_error")

processed = tiles | 'Robust Processing' >> beam.Map(robust_raster_processing)
```

## Integration Examples

### Satellite Imagery Processing

```python
# Landsat 8 NDVI calculation pipeline
def create_landsat_ndvi_pipeline():
    return (
        pipeline
        | 'Read Landsat' >> GeoTIFFTransform(
            'landsat8/*.tif',
            bands=[4, 5],  # Red and NIR bands
            tile_size=(1024, 1024)
        )
        | 'Calculate NDVI' >> RasterBandMathTransform(
            expression="(B5 - B4) / (B5 + B4)",
            output_name="ndvi"
        )
        | 'Filter Valid' >> beam.Filter(lambda x: x.data.min() > -1)
        | 'Write Results' >> WriteRasterToBigQuery()
    )
```

### Digital Elevation Model Analysis

```python
# DEM slope calculation
def create_slope_analysis_pipeline():
    return (
        pipeline
        | 'Read DEM' >> GeoTIFFTransform('elevation.tif')
        | 'Calculate Slope' >> beam.Map(calculate_slope)
        | 'Classify Slopes' >> beam.Map(classify_slope_categories)
        | 'Write Results' >> beam.io.WriteToText('slope_results.txt')
    )

def calculate_slope(tile):
    """Calculate slope from elevation data."""
    elevation = tile.data[0].astype(np.float32)
    
    # Simple gradient calculation
    dy, dx = np.gradient(elevation)
    slope = np.arctan(np.sqrt(dx*dx + dy*dy)) * 180 / np.pi
    
    return tile._replace(data=slope[np.newaxis, ...])
```

### BigQuery Integration

```python
from apache_beam.io.gcp.bigquery import WriteToBigQuery

def tile_to_bigquery_row(tile):
    """Convert raster tile to BigQuery row format."""
    # Calculate statistics for the tile
    stats = {
        'tile_id': tile.tile_id,
        'mean_value': float(np.nanmean(tile.data)),
        'std_value': float(np.nanstd(tile.data)),
        'min_value': float(np.nanmin(tile.data)),
        'max_value': float(np.nanmax(tile.data)),
        'valid_pixels': int(np.sum(~np.isnan(tile.data))),
        'bounds': list(tile.window),
        'processed_at': beam.utils.timestamp.Timestamp.now().to_rfc3339()
    }
    return stats

# Define BigQuery schema
table_schema = {
    'fields': [
        {'name': 'tile_id', 'type': 'STRING'},
        {'name': 'mean_value', 'type': 'FLOAT'},
        {'name': 'std_value', 'type': 'FLOAT'},
        {'name': 'min_value', 'type': 'FLOAT'},
        {'name': 'max_value', 'type': 'FLOAT'},
        {'name': 'valid_pixels', 'type': 'INTEGER'},
        {'name': 'bounds', 'type': 'FLOAT', 'mode': 'REPEATED'},
        {'name': 'processed_at', 'type': 'TIMESTAMP'}
    ]
}

# Pipeline
tile_stats = (
    pipeline
    | 'Read Raster' >> GeoTIFFTransform('gs://bucket/data/*.tif')
    | 'Calculate Stats' >> beam.Map(tile_to_bigquery_row)
    | 'Write to BigQuery' >> WriteToBigQuery(
        table='project:dataset.raster_stats',
        schema=table_schema,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
    )
)
```

### Dataflow Integration

```python
from apache_beam.options.pipeline_options import PipelineOptions

# Configure Dataflow options
options = PipelineOptions([
    '--project=my-project',
    '--region=us-central1',
    '--runner=DataflowRunner',
    '--temp_location=gs://my-bucket/temp',
    '--staging_location=gs://my-bucket/staging',
    '--max_num_workers=20'
])

# Create pipeline with Dataflow
processor = GeoProcessor()
pipeline = processor.create_pipeline(options)

processed_tiles = (
    pipeline
    | 'Read Satellite Data' >> GeoTIFFTransform(
        'gs://satellite-data/*.tif',
        use_splittable=True
    )
    | 'Calculate Vegetation Indices' >> RasterBandMathTransform(
        expression="(B4 - B3) / (B4 + B3)",
        output_name="ndvi"
    )
    | 'Filter Vegetation' >> beam.Filter(lambda x: np.nanmean(x.data) > 0.3)
    | 'Write Results' >> WriteToBigQuery('project:dataset.vegetation_analysis')
)

result = pipeline.run()
result.wait_until_finish()
```

## Troubleshooting

### Common Issues

1. **Memory Issues**: Use smaller tile sizes and ensure streaming processing
2. **Performance Issues**: Enable splittable processing and use appropriate tile sizes
3. **Data Issues**: Validate input data and handle nodata values properly
4. **File Access Issues**: Check file permissions and cloud storage credentials

### Debug Mode

```python
import logging
logging.basicConfig(level=logging.DEBUG)

# Enable detailed logging
reader = GeoTIFFReader('data.tif', validate_cog=True)
```

### Performance Monitoring

```python
import time

def timed_process_tile(tile):
    start_time = time.time()
    result = process_tile(tile)
    duration = time.time() - start_time
    if duration > 5.0:  # Log slow tiles
        logging.warning(f"Slow tile processing: {duration:.2f}s for {tile.tile_id}")
    return result

tiles = (
    pipeline
    | 'Read Data' >> GeoTIFFTransform('data.tif')
    | 'Timed Process' >> beam.Map(timed_process_tile)
)
```

## API Reference

### Classes

- `GeoTIFFReader`: Core reader class for GeoTIFF files
- `GeoTIFFTransform`: Apache Beam transform for reading GeoTIFF files
- `ReadGeoTIFFTilesFn`: DoFn for processing files as tiles
- `ReadGeoTIFFBandsFn`: DoFn for processing files as bands
- `SplittableGeoTIFFReaderFn`: Splittable DoFn for parallel processing

### Raster Transforms

- `RasterBandMathTransform`: Perform band math operations
- `RasterResampleTransform`: Resample raster to different resolution
- `ZonalStatisticsTransform`: Calculate statistics within vector boundaries
- `RasterFilterTransform`: Filter raster data based on criteria
- `RasterMosaicTransform`: Mosaic multiple raster tiles

### Analytics

- `VegetationIndices`: Vegetation index calculations (NDVI, EVI, SAVI, etc.)
- `WaterIndices`: Water index calculations (NDWI, MNDWI)
- `UrbanIndices`: Urban index calculations (NDBI, UI)
- `TemporalRasterAnalysis`: Temporal analysis for time series data
- `ChangeDetectionAnalysis`: Change detection between time periods

For detailed API documentation, see the docstrings in the source code.

# FlatGeobuf Reader for GeXus Framework

## Overview

The FlatGeobuf reader is a production-ready implementation for processing FlatGeobuf format files within the GeXus framework. It provides seamless integration with Apache Beam pipelines for scalable geospatial data processing.

## Features

- **Streaming Processing**: Memory-efficient streaming of features without loading entire files
- **Apache Beam Integration**: Native support for Beam pipelines with parallel processing
- **Splittable DoFn**: Automatic splitting of large files for distributed processing
- **Spatial Filtering**: Built-in spatial filtering using bounding boxes
- **Geometry Validation**: Optional geometry validation and repair
- **CRS Support**: Coordinate Reference System handling and transformations
- **Error Handling**: Robust error handling for corrupted or invalid files

## Quick Start

### Basic Usage

```python
from gexus.core import GeoProcessor
from gexus.formats.flatgeobuf import FlatGeobufTransform

# Create processor and pipeline
processor = GeoProcessor()
pipeline = processor.create_pipeline()

# Read FlatGeobuf data
features = (
    pipeline 
    | 'Read Data' >> FlatGeobufTransform('path/to/data/*.fgb')
    | 'Process Features' >> beam.Map(lambda x: process_feature(x))
)

# Run pipeline
result = pipeline.run()
```

### Direct Reader Usage

```python
from gexus.formats.flatgeobuf import FlatGeobufReader

# Create reader
reader = FlatGeobufReader('data.fgb')

# Get schema information
schema = reader.get_schema()
print(f"Geometry type: {schema['geometry_type']}")
print(f"Properties: {schema['properties']}")

# Get spatial bounds
bounds = reader.get_bounds()
print(f"Bounds: {bounds}")  # (minx, miny, maxx, maxy)

# Read features
for feature in reader.read_features():
    print(f"Feature: {feature['properties']['name']}")
    print(f"Geometry: {feature['geometry']['type']}")
```

## Advanced Usage

### Spatial Filtering

```python
from gexus.formats.flatgeobuf import FlatGeobufTransform
from gexus.transforms.spatial import SpatialFilterTransform

# Read with bounding box filter
bbox = (-122.5, 37.7, -122.3, 37.8)  # San Francisco area
features = (
    pipeline
    | 'Read Data' >> FlatGeobufTransform('cities.fgb', bbox=bbox)
    | 'Additional Filter' >> SpatialFilterTransform(bbox=bbox)
)
```

### Geometry Validation

```python
from gexus.transforms.spatial import GeometryValidationTransform

features = (
    pipeline
    | 'Read Data' >> FlatGeobufTransform('data.fgb')
    | 'Validate Geometries' >> GeometryValidationTransform(
        fix_invalid=True,
        remove_invalid=False
    )
)
```

### CRS Transformation

```python
from gexus.transforms.spatial import CRSTransformTransform

features = (
    pipeline
    | 'Read Data' >> FlatGeobufTransform('data.fgb')
    | 'Transform CRS' >> CRSTransformTransform(
        source_crs='EPSG:4326',
        target_crs='EPSG:3857'
    )
)
```

### Property Filtering

```python
# Read only specific properties
features = (
    pipeline
    | 'Read Data' >> FlatGeobufTransform(
        'data.fgb',
        properties=['name', 'population']  # Only include these properties
    )
)
```

### Parallel Processing

```python
# Enable splittable DoFn for large files
features = (
    pipeline
    | 'Read Data' >> FlatGeobufTransform(
        'large_file.fgb',
        use_splittable=True  # Default is True
    )
)

# Disable splittable processing for small files
features = (
    pipeline
    | 'Read Data' >> FlatGeobufTransform(
        'small_file.fgb',
        use_splittable=False
    )
)
```

## Configuration Options

### FlatGeobufReader Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `bbox` | `Tuple[float, float, float, float]` | `None` | Spatial filter bounding box (minx, miny, maxx, maxy) |
| `properties` | `List[str]` | `None` | List of property names to include (None for all) |
| `validate_geometry` | `bool` | `True` | Whether to validate geometries |
| `encoding` | `str` | `'utf-8'` | Text encoding for string properties |

### FlatGeobufTransform Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `use_splittable` | `bool` | `True` | Whether to use splittable DoFn for parallel processing |
| All FlatGeobufReader options | | | Passed through to the reader |

## Performance Tuning

### Memory Usage

The FlatGeobuf reader is designed for constant memory usage regardless of file size:

```python
# Memory-efficient streaming
reader = FlatGeobufReader('large_file.fgb')
for feature in reader.read_features():
    # Process one feature at a time
    process_feature(feature)
```

### Parallel Processing

For large files, enable splittable processing:

```python
# Automatic splitting for parallel processing
features = (
    pipeline
    | 'Read Large File' >> FlatGeobufTransform(
        'very_large_file.fgb',
        use_splittable=True
    )
)
```

### Spatial Indexing

Leverage FlatGeobuf's built-in spatial index:

```python
# Use bounding box for efficient spatial filtering
bbox = (-180, -90, 180, 90)  # World bounds
reader = FlatGeobufReader('data.fgb', bbox=bbox)
```

## Error Handling

### File Errors

```python
try:
    reader = FlatGeobufReader('nonexistent.fgb')
except FileNotFoundError as e:
    print(f"File not found: {e}")
```

### Geometry Errors

```python
# Handle invalid geometries
reader = FlatGeobufReader(
    'data.fgb',
    validate_geometry=True  # Attempt to fix invalid geometries
)

for feature in reader.read_features():
    if feature['geometry'] is None:
        print("Skipping feature with invalid geometry")
        continue
    # Process valid feature
```

### Pipeline Errors

```python
def safe_process_feature(feature):
    try:
        return process_feature(feature)
    except Exception as e:
        logging.warning(f"Failed to process feature: {e}")
        return None

features = (
    pipeline
    | 'Read Data' >> FlatGeobufTransform('data.fgb')
    | 'Safe Process' >> beam.Map(safe_process_feature)
    | 'Filter None' >> beam.Filter(lambda x: x is not None)
)
```

## Integration Examples

### BigQuery Integration

```python
import apache_beam as beam
from apache_beam.io.gcp.bigquery import WriteToBigQuery

def feature_to_bigquery_row(feature):
    """Convert feature to BigQuery row format."""
    return {
        'geometry': json.dumps(feature['geometry']),
        'properties': json.dumps(feature['properties']),
        'name': feature['properties'].get('name'),
        'created_at': beam.utils.timestamp.Timestamp.now().to_rfc3339()
    }

# Define BigQuery schema
table_schema = {
    'fields': [
        {'name': 'geometry', 'type': 'STRING'},
        {'name': 'properties', 'type': 'STRING'},
        {'name': 'name', 'type': 'STRING'},
        {'name': 'created_at', 'type': 'TIMESTAMP'}
    ]
}

# Pipeline
features = (
    pipeline
    | 'Read FlatGeobuf' >> FlatGeobufTransform('gs://bucket/data/*.fgb')
    | 'Convert to BQ Format' >> beam.Map(feature_to_bigquery_row)
    | 'Write to BigQuery' >> WriteToBigQuery(
        table='project:dataset.table',
        schema=table_schema,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
    )
)
```

### Cloud Storage Integration

```python
# Read from Google Cloud Storage
features = (
    pipeline
    | 'Read from GCS' >> FlatGeobufTransform('gs://my-bucket/data/*.fgb')
    | 'Process Features' >> beam.Map(process_feature)
)

# Read from multiple sources
features = (
    pipeline
    | 'Read Multiple Sources' >> FlatGeobufTransform([
        'gs://bucket1/data/*.fgb',
        'gs://bucket2/data/*.fgb',
        '/local/path/*.fgb'
    ])
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
    '--staging_location=gs://my-bucket/staging'
])

# Create pipeline with Dataflow
processor = GeoProcessor()
pipeline = processor.create_pipeline(options)

features = (
    pipeline
    | 'Read FlatGeobuf' >> FlatGeobufTransform('gs://data/*.fgb')
    | 'Process at Scale' >> beam.Map(process_feature)
    | 'Write Results' >> WriteToBigQuery('project:dataset.results')
)

result = pipeline.run()
result.wait_until_finish()
```

## Troubleshooting

### Common Issues

1. **Memory Issues**: Ensure you're using streaming processing and not collecting all features in memory
2. **Performance Issues**: Enable splittable processing for large files
3. **Geometry Issues**: Enable geometry validation and error handling
4. **File Access Issues**: Check file permissions and paths

### Debug Mode

```python
import logging
logging.basicConfig(level=logging.DEBUG)

# Enable detailed logging
reader = FlatGeobufReader('data.fgb', validate_geometry=True)
```

### Performance Monitoring

```python
import time

def timed_process_feature(feature):
    start_time = time.time()
    result = process_feature(feature)
    duration = time.time() - start_time
    if duration > 1.0:  # Log slow features
        logging.warning(f"Slow feature processing: {duration:.2f}s")
    return result

features = (
    pipeline
    | 'Read Data' >> FlatGeobufTransform('data.fgb')
    | 'Timed Process' >> beam.Map(timed_process_feature)
)
```

## API Reference

### Classes

- `FlatGeobufReader`: Core reader class for FlatGeobuf files
- `FlatGeobufTransform`: Apache Beam transform for reading FlatGeobuf files
- `ReadFlatGeobufFn`: DoFn for processing individual files
- `SplittableFlatGeobufFn`: Splittable DoFn for parallel processing

### Spatial Transforms

- `SpatialFilterTransform`: Filter features by spatial criteria
- `GeometryValidationTransform`: Validate and fix geometries
- `CRSTransformTransform`: Transform coordinate reference systems
- `BoundsCalculationTransform`: Calculate spatial bounds

For detailed API documentation, see the docstrings in the source code.

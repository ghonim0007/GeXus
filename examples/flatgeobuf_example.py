#!/usr/bin/env python3
"""
FlatGeobuf Reader Example for GeXus Framework

This example demonstrates how to use the FlatGeobuf reader with Apache Beam
for scalable geospatial data processing.
"""

import json
import logging
from typing import Dict, Any

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from gexus.core.processor import GeoProcessor
from gexus.formats.flatgeobuf import FlatGeobufTransform
from gexus.transforms.spatial import (
    SpatialFilterTransform,
    GeometryValidationTransform,
    CRSTransformTransform,
    BoundsCalculationTransform
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def process_feature(feature: Dict[str, Any]) -> Dict[str, Any]:
    """Process a single feature with custom logic.
    
    Args:
        feature: Feature dictionary with geometry and properties
        
    Returns:
        Processed feature dictionary
    """
    # Add computed properties
    properties = feature['properties']
    
    # Add feature area if it's a polygon
    if feature['geometry']['type'] in ['Polygon', 'MultiPolygon']:
        try:
            from shapely.geometry import shape
            geom = shape(feature['geometry'])
            properties['area_m2'] = geom.area
        except Exception as e:
            logger.warning(f"Failed to calculate area: {e}")
            properties['area_m2'] = None
    
    # Add centroid coordinates
    try:
        from shapely.geometry import shape
        geom = shape(feature['geometry'])
        centroid = geom.centroid
        properties['centroid_lon'] = centroid.x
        properties['centroid_lat'] = centroid.y
    except Exception as e:
        logger.warning(f"Failed to calculate centroid: {e}")
        properties['centroid_lon'] = None
        properties['centroid_lat'] = None
    
    # Add processing timestamp
    import datetime
    properties['processed_at'] = datetime.datetime.now().isoformat()
    
    return feature


def filter_by_population(feature: Dict[str, Any]) -> bool:
    """Filter features by population threshold.
    
    Args:
        feature: Feature dictionary
        
    Returns:
        True if feature should be included
    """
    population = feature['properties'].get('population', 0)
    return population > 100000  # Cities with population > 100k


def create_summary_stats(features):
    """Create summary statistics from features.
    
    Args:
        features: Iterable of feature dictionaries
        
    Returns:
        Summary statistics dictionary
    """
    total_features = 0
    total_population = 0
    geometry_types = {}
    
    for feature in features:
        total_features += 1
        
        # Count population
        population = feature['properties'].get('population', 0)
        if isinstance(population, (int, float)):
            total_population += population
        
        # Count geometry types
        geom_type = feature['geometry']['type']
        geometry_types[geom_type] = geometry_types.get(geom_type, 0) + 1
    
    return {
        'total_features': total_features,
        'total_population': total_population,
        'geometry_types': geometry_types,
        'average_population': total_population / total_features if total_features > 0 else 0
    }


def example_basic_usage():
    """Example of basic FlatGeobuf reader usage."""
    logger.info("=== Basic Usage Example ===")
    
    # Create processor and pipeline
    processor = GeoProcessor()
    pipeline = processor.create_pipeline()
    
    # Read FlatGeobuf data
    features = (
        pipeline
        | 'Read FlatGeobuf' >> FlatGeobufTransform('data/*.fgb')
        | 'Process Features' >> beam.Map(process_feature)
        | 'Log Features' >> beam.Map(lambda f: logger.info(f"Processed: {f['properties'].get('name', 'Unknown')}") or f)
    )
    
    # Run pipeline
    result = pipeline.run()
    result.wait_until_finish()
    logger.info("Basic usage example completed")


def example_spatial_filtering():
    """Example of spatial filtering with FlatGeobuf reader."""
    logger.info("=== Spatial Filtering Example ===")
    
    processor = GeoProcessor()
    pipeline = processor.create_pipeline()
    
    # Define bounding box for San Francisco Bay Area
    sf_bay_area_bbox = (-122.6, 37.4, -121.8, 37.9)
    
    features = (
        pipeline
        | 'Read FlatGeobuf' >> FlatGeobufTransform(
            'cities.fgb',
            bbox=sf_bay_area_bbox  # Pre-filter at read time
        )
        | 'Additional Spatial Filter' >> SpatialFilterTransform(bbox=sf_bay_area_bbox)
        | 'Validate Geometries' >> GeometryValidationTransform(fix_invalid=True)
        | 'Process Features' >> beam.Map(process_feature)
        | 'Filter by Population' >> beam.Filter(filter_by_population)
        | 'Log Results' >> beam.Map(lambda f: logger.info(f"Bay Area city: {f['properties'].get('name')}") or f)
    )
    
    result = pipeline.run()
    result.wait_until_finish()
    logger.info("Spatial filtering example completed")


def example_crs_transformation():
    """Example of coordinate reference system transformation."""
    logger.info("=== CRS Transformation Example ===")
    
    processor = GeoProcessor()
    pipeline = processor.create_pipeline()
    
    features = (
        pipeline
        | 'Read FlatGeobuf' >> FlatGeobufTransform('data.fgb')
        | 'Transform to Web Mercator' >> CRSTransformTransform(
            source_crs='EPSG:4326',  # WGS84
            target_crs='EPSG:3857'   # Web Mercator
        )
        | 'Process Features' >> beam.Map(process_feature)
        | 'Log Transformed' >> beam.Map(lambda f: logger.info(f"Transformed: {f['properties'].get('name')}") or f)
    )
    
    result = pipeline.run()
    result.wait_until_finish()
    logger.info("CRS transformation example completed")


def example_aggregation():
    """Example of feature aggregation and statistics."""
    logger.info("=== Aggregation Example ===")
    
    processor = GeoProcessor()
    pipeline = processor.create_pipeline()
    
    # Read and process features
    features = (
        pipeline
        | 'Read FlatGeobuf' >> FlatGeobufTransform('cities.fgb')
        | 'Validate Geometries' >> GeometryValidationTransform()
        | 'Process Features' >> beam.Map(process_feature)
    )
    
    # Calculate total population
    total_population = (
        features
        | 'Extract Population' >> beam.Map(lambda f: f['properties'].get('population', 0))
        | 'Sum Population' >> beam.CombineGlobally(sum)
        | 'Log Total Population' >> beam.Map(lambda x: logger.info(f"Total population: {x:,}") or x)
    )
    
    # Count features by geometry type
    geometry_counts = (
        features
        | 'Extract Geometry Type' >> beam.Map(lambda f: f['geometry']['type'])
        | 'Count by Type' >> beam.combiners.Count.PerElement()
        | 'Log Geometry Counts' >> beam.Map(lambda x: logger.info(f"Geometry type {x[0]}: {x[1]} features") or x)
    )
    
    # Group by state/region
    state_populations = (
        features
        | 'Extract State Population' >> beam.Map(lambda f: (
            f['properties'].get('state', 'Unknown'),
            f['properties'].get('population', 0)
        ))
        | 'Group by State' >> beam.GroupByKey()
        | 'Sum by State' >> beam.Map(lambda x: (x[0], sum(x[1])))
        | 'Log State Populations' >> beam.Map(lambda x: logger.info(f"State {x[0]}: {x[1]:,} population") or x)
    )
    
    result = pipeline.run()
    result.wait_until_finish()
    logger.info("Aggregation example completed")


def example_bigquery_integration():
    """Example of BigQuery integration."""
    logger.info("=== BigQuery Integration Example ===")
    
    try:
        from apache_beam.io.gcp.bigquery import WriteToBigQuery
    except ImportError:
        logger.warning("BigQuery integration not available - skipping example")
        return
    
    def feature_to_bigquery_row(feature: Dict[str, Any]) -> Dict[str, Any]:
        """Convert feature to BigQuery row format."""
        return {
            'feature_id': feature['properties'].get('id', 'unknown'),
            'name': feature['properties'].get('name', 'Unknown'),
            'geometry_type': feature['geometry']['type'],
            'geometry_json': json.dumps(feature['geometry']),
            'properties_json': json.dumps(feature['properties']),
            'population': feature['properties'].get('population'),
            'area_m2': feature['properties'].get('area_m2'),
            'centroid_lon': feature['properties'].get('centroid_lon'),
            'centroid_lat': feature['properties'].get('centroid_lat'),
            'processed_at': feature['properties'].get('processed_at')
        }
    
    # BigQuery table schema
    table_schema = {
        'fields': [
            {'name': 'feature_id', 'type': 'STRING'},
            {'name': 'name', 'type': 'STRING'},
            {'name': 'geometry_type', 'type': 'STRING'},
            {'name': 'geometry_json', 'type': 'STRING'},
            {'name': 'properties_json', 'type': 'STRING'},
            {'name': 'population', 'type': 'INTEGER'},
            {'name': 'area_m2', 'type': 'FLOAT'},
            {'name': 'centroid_lon', 'type': 'FLOAT'},
            {'name': 'centroid_lat', 'type': 'FLOAT'},
            {'name': 'processed_at', 'type': 'TIMESTAMP'}
        ]
    }
    
    processor = GeoProcessor()
    pipeline = processor.create_pipeline()
    
    features = (
        pipeline
        | 'Read FlatGeobuf' >> FlatGeobufTransform('data/*.fgb')
        | 'Process Features' >> beam.Map(process_feature)
        | 'Convert to BQ Format' >> beam.Map(feature_to_bigquery_row)
        | 'Write to BigQuery' >> WriteToBigQuery(
            table='your-project:your_dataset.processed_features',
            schema=table_schema,
            write_disposition='WRITE_TRUNCATE',
            create_disposition='CREATE_IF_NEEDED'
        )
    )
    
    result = pipeline.run()
    result.wait_until_finish()
    logger.info("BigQuery integration example completed")


def example_dataflow_runner():
    """Example of running on Google Cloud Dataflow."""
    logger.info("=== Dataflow Runner Example ===")
    
    # Configure Dataflow options
    options = PipelineOptions([
        '--project=your-project-id',
        '--region=us-central1',
        '--runner=DataflowRunner',
        '--temp_location=gs://your-bucket/temp',
        '--staging_location=gs://your-bucket/staging',
        '--setup_file=./setup.py'
    ])
    
    processor = GeoProcessor()
    pipeline = processor.create_pipeline(options)
    
    features = (
        pipeline
        | 'Read from GCS' >> FlatGeobufTransform(
            'gs://your-bucket/data/*.fgb',
            use_splittable=True  # Enable parallel processing
        )
        | 'Spatial Filter' >> SpatialFilterTransform(bbox=(-180, -90, 180, 90))
        | 'Validate Geometries' >> GeometryValidationTransform(fix_invalid=True)
        | 'Process Features' >> beam.Map(process_feature)
        | 'Filter Large Cities' >> beam.Filter(filter_by_population)
        | 'Calculate Bounds' >> BoundsCalculationTransform()
        | 'Extract Features' >> beam.Map(lambda x: x[0])  # Extract feature from (feature, bounds) tuple
    )
    
    # Note: This would actually submit to Dataflow
    # result = pipeline.run()
    # result.wait_until_finish()
    
    logger.info("Dataflow runner example configured (not executed)")


def example_error_handling():
    """Example of robust error handling."""
    logger.info("=== Error Handling Example ===")
    
    def safe_process_feature(feature: Dict[str, Any]) -> Dict[str, Any]:
        """Safely process a feature with error handling."""
        try:
            return process_feature(feature)
        except Exception as e:
            logger.warning(f"Failed to process feature: {e}")
            # Return feature with error flag
            feature['properties']['processing_error'] = str(e)
            return feature
    
    def log_processing_errors(feature: Dict[str, Any]) -> Dict[str, Any]:
        """Log features that had processing errors."""
        if 'processing_error' in feature['properties']:
            logger.error(f"Feature processing error: {feature['properties']['processing_error']}")
        return feature
    
    processor = GeoProcessor()
    pipeline = processor.create_pipeline()
    
    features = (
        pipeline
        | 'Read FlatGeobuf' >> FlatGeobufTransform(
            'data/*.fgb',
            validate_geometry=True  # Enable geometry validation
        )
        | 'Safe Process' >> beam.Map(safe_process_feature)
        | 'Log Errors' >> beam.Map(log_processing_errors)
        | 'Filter Valid Features' >> beam.Filter(
            lambda f: 'processing_error' not in f['properties']
        )
    )
    
    result = pipeline.run()
    result.wait_until_finish()
    logger.info("Error handling example completed")


def main():
    """Run all examples."""
    logger.info("Starting FlatGeobuf Reader Examples")
    
    # Note: These examples assume you have FlatGeobuf files available
    # Comment out examples that require specific data files
    
    try:
        example_basic_usage()
    except Exception as e:
        logger.error(f"Basic usage example failed: {e}")
    
    try:
        example_spatial_filtering()
    except Exception as e:
        logger.error(f"Spatial filtering example failed: {e}")
    
    try:
        example_crs_transformation()
    except Exception as e:
        logger.error(f"CRS transformation example failed: {e}")
    
    try:
        example_aggregation()
    except Exception as e:
        logger.error(f"Aggregation example failed: {e}")
    
    try:
        example_bigquery_integration()
    except Exception as e:
        logger.error(f"BigQuery integration example failed: {e}")
    
    try:
        example_dataflow_runner()
    except Exception as e:
        logger.error(f"Dataflow runner example failed: {e}")
    
    try:
        example_error_handling()
    except Exception as e:
        logger.error(f"Error handling example failed: {e}")
    
    logger.info("All examples completed")


if __name__ == '__main__':
    main()

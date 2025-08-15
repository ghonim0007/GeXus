import pytest
import tempfile
import shutil
from pathlib import Path

@pytest.fixture
def temp_dir():
    """Create a temporary directory for tests."""
    temp_path = Path(tempfile.mkdtemp())
    yield temp_path
    shutil.rmtree(temp_path)

@pytest.fixture
def sample_geojson():
    """Sample GeoJSON data for testing."""
    return {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [31.2357, 30.0444]  # Cairo coordinates
                },
                "properties": {
                    "name": "Cairo",
                    "population": 9500000
                }
            }
        ]
    }
import pytest
from gexus.core import GeoProcessor

class TestGeoProcessor:
    def test_processor_initialization(self):
        """Test that GeoProcessor can be initialized."""
        processor = GeoProcessor()
        assert processor is not None
    
    def test_processor_with_config(self):
        """Test processor with configuration."""
        config = {"format": "flatgeobuf"}
        processor = GeoProcessor(config=config)
        assert processor.config["format"] == "flatgeobuf"
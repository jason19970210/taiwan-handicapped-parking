"""
Tests for geocoding utilities.
"""

import pytest
from scripts.utils.geocoding import (
    decimal_to_dms,
    dms_to_decimal,
    CoordinateConverter,
    validate_coordinates,
    is_in_taiwan
)


class TestDecimalToDMS:
    """Tests for decimal to DMS conversion."""

    def test_positive_latitude(self):
        """Test conversion of positive latitude."""
        result = decimal_to_dms(25.0330, is_latitude=True)
        assert result.startswith("25°")
        assert result.endswith("N")
        assert "01'" in result

    def test_negative_latitude(self):
        """Test conversion of negative latitude."""
        result = decimal_to_dms(-25.0330, is_latitude=True)
        assert result.startswith("25°")
        assert result.endswith("S")

    def test_positive_longitude(self):
        """Test conversion of positive longitude."""
        result = decimal_to_dms(121.5654, is_latitude=False)
        assert result.startswith("121°")
        assert result.endswith("E")
        assert "33'" in result

    def test_negative_longitude(self):
        """Test conversion of negative longitude."""
        result = decimal_to_dms(-121.5654, is_latitude=False)
        assert result.startswith("121°")
        assert result.endswith("W")

    def test_zero_coordinate(self):
        """Test conversion of zero coordinate."""
        result_lat = decimal_to_dms(0.0, is_latitude=True)
        assert result_lat == '0°00\'00.00"N'

        result_lon = decimal_to_dms(0.0, is_latitude=False)
        assert result_lon == '0°00\'00.00"E'


class TestCoordinateValidation:
    """Tests for coordinate validation."""

    def test_valid_coordinates(self):
        """Test validation of valid coordinates."""
        assert validate_coordinates(25.0330, 121.5654) is True
        assert validate_coordinates(0, 0) is True
        assert validate_coordinates(-90, -180) is True
        assert validate_coordinates(90, 180) is True

    def test_invalid_latitude(self):
        """Test validation of invalid latitude."""
        assert validate_coordinates(91, 0) is False
        assert validate_coordinates(-91, 0) is False

    def test_invalid_longitude(self):
        """Test validation of invalid longitude."""
        assert validate_coordinates(0, 181) is False
        assert validate_coordinates(0, -181) is False


class TestTaiwanBounds:
    """Tests for Taiwan bounds checking."""

    def test_taipei_in_taiwan(self):
        """Test that Taipei coordinates are in Taiwan."""
        assert is_in_taiwan(25.0330, 121.5654) is True

    def test_kaohsiung_in_taiwan(self):
        """Test that Kaohsiung coordinates are in Taiwan."""
        assert is_in_taiwan(22.6273, 120.3014) is True

    def test_tokyo_not_in_taiwan(self):
        """Test that Tokyo coordinates are not in Taiwan."""
        assert is_in_taiwan(35.6762, 139.6503) is False

    def test_beijing_not_in_taiwan(self):
        """Test that Beijing coordinates are not in Taiwan."""
        assert is_in_taiwan(39.9042, 116.4074) is False


@pytest.mark.slow
class TestCoordinateConverter:
    """Tests for coordinate system conversion."""

    def test_twd97_to_wgs84(self):
        """Test TWD97 to WGS84 conversion."""
        # Sample TWD97 coordinates (approximate)
        x, y = 302520, 2771050

        lat, lon = CoordinateConverter.twd97_to_wgs84(x, y)

        # Check that results are in Taiwan bounds
        assert is_in_taiwan(lat, lon)
        assert validate_coordinates(lat, lon)

        # Check approximate expected values (within 0.01 degree)
        assert abs(lat - 25.0330) < 0.01
        assert abs(lon - 121.5654) < 0.01

    def test_wgs84_to_twd97(self):
        """Test WGS84 to TWD97 conversion."""
        lat, lon = 25.0330, 121.5654

        x, y = CoordinateConverter.wgs84_to_twd97(lat, lon)

        # Check approximate expected values (within 1000 meters)
        assert abs(x - 302520) < 1000
        assert abs(y - 2771050) < 1000

    def test_round_trip_conversion(self):
        """Test that converting back and forth preserves coordinates."""
        original_lat, original_lon = 25.0330, 121.5654

        # Convert to TWD97 and back
        x, y = CoordinateConverter.wgs84_to_twd97(original_lat, original_lon)
        lat, lon = CoordinateConverter.twd97_to_wgs84(x, y)

        # Should be very close to original (within 0.0001 degree ~ 10 meters)
        assert abs(lat - original_lat) < 0.0001
        assert abs(lon - original_lon) < 0.0001

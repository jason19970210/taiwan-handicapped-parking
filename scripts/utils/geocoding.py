"""
Geocoding utilities for coordinate conversion.
Handles decimal degrees (DD) to degrees-minutes-seconds (DMS) conversion
and coordinate system transformations (TWD97 to WGS84).
"""

import math
from typing import Tuple


def decimal_to_dms(decimal: float, is_latitude: bool = True) -> str:
    """
    Convert decimal degrees to DMS (Degrees, Minutes, Seconds) format.

    Args:
        decimal: Decimal degree value
        is_latitude: True for latitude, False for longitude

    Returns:
        DMS string in format: "25°02'23.40"N" or "121°33'55.44"E"

    Examples:
        >>> decimal_to_dms(25.0330, is_latitude=True)
        '25°01\'58.80"N'
        >>> decimal_to_dms(121.5654, is_latitude=False)
        '121°33\'55.44"E'
        >>> decimal_to_dms(-25.0330, is_latitude=True)
        '25°01\'58.80"S'
    """
    is_positive = decimal >= 0
    decimal = abs(decimal)

    degrees = int(decimal)
    minutes_decimal = (decimal - degrees) * 60
    minutes = int(minutes_decimal)
    seconds = (minutes_decimal - minutes) * 60

    # Determine direction
    if is_latitude:
        direction = 'N' if is_positive else 'S'
    else:
        direction = 'E' if is_positive else 'W'

    return f"{degrees}°{minutes:02d}'{seconds:05.2f}\"{direction}"


def dms_to_decimal(dms: str) -> float:
    """
    Convert DMS string to decimal degrees.

    Args:
        dms: DMS string in format like "25°01'58.80"N"

    Returns:
        Decimal degree value

    Examples:
        >>> dms_to_decimal('25°01\'58.80"N')
        25.033
    """
    # Parse the DMS string
    direction = dms[-1]
    dms = dms[:-1]  # Remove direction

    # Split by degree, minute, second symbols
    parts = dms.replace('°', ' ').replace("'", ' ').replace('"', '').split()

    degrees = float(parts[0])
    minutes = float(parts[1]) if len(parts) > 1 else 0
    seconds = float(parts[2]) if len(parts) > 2 else 0

    # Calculate decimal
    decimal = degrees + minutes / 60 + seconds / 3600

    # Apply direction
    if direction in ['S', 'W']:
        decimal = -decimal

    return decimal


class CoordinateConverter:
    """Coordinate system converter for Taiwan projections."""

    @staticmethod
    def twd97_to_wgs84(x: float, y: float) -> Tuple[float, float]:
        """
        Convert TWD97 (Taiwan Datum 1997) coordinates to WGS84.

        TWD97 is commonly used in Taiwan government data.
        EPSG:3826 = TWD97 / TM2 zone 121
        EPSG:4326 = WGS84 (used by Google Maps)

        Args:
            x: TWD97 X coordinate (easting)
            y: TWD97 Y coordinate (northing)

        Returns:
            Tuple of (latitude, longitude) in WGS84

        Examples:
            >>> lat, lon = CoordinateConverter.twd97_to_wgs84(302520, 2771050)
            >>> round(lat, 4), round(lon, 4)
            (25.0330, 121.5654)
        """
        try:
            from pyproj import Transformer
            transformer = Transformer.from_crs("EPSG:3826", "EPSG:4326", always_xy=False)
            lat, lon = transformer.transform(y, x)  # Note: transform expects (lat/y, lon/x)
            return lat, lon
        except ImportError:
            raise ImportError(
                "pyproj is required for coordinate conversion. "
                "Install it with: pip install pyproj"
            )

    @staticmethod
    def wgs84_to_twd97(lat: float, lon: float) -> Tuple[float, float]:
        """
        Convert WGS84 coordinates to TWD97.

        Args:
            lat: Latitude in WGS84
            lon: Longitude in WGS84

        Returns:
            Tuple of (x, y) in TWD97
        """
        try:
            from pyproj import Transformer
            transformer = Transformer.from_crs("EPSG:4326", "EPSG:3826", always_xy=False)
            y, x = transformer.transform(lat, lon)
            return x, y
        except ImportError:
            raise ImportError(
                "pyproj is required for coordinate conversion. "
                "Install it with: pip install pyproj"
            )


def validate_coordinates(lat: float, lon: float) -> bool:
    """
    Validate that coordinates are within valid ranges.

    Args:
        lat: Latitude
        lon: Longitude

    Returns:
        True if valid, False otherwise
    """
    return -90 <= lat <= 90 and -180 <= lon <= 180


def is_in_taiwan(lat: float, lon: float) -> bool:
    """
    Check if coordinates are approximately within Taiwan's bounds.

    Taiwan approximate bounds:
    - Latitude: 21.5°N to 25.5°N
    - Longitude: 119.5°E to 122.5°E

    Args:
        lat: Latitude in WGS84
        lon: Longitude in WGS84

    Returns:
        True if coordinates are within Taiwan bounds
    """
    return 21.5 <= lat <= 25.5 and 119.5 <= lon <= 122.5

"""
Taipei City handicapped parking data handler.
Downloads shapefile data, filters by parking type, and converts to standard format.
"""

import geopandas as gpd
import pandas as pd
import requests
import zipfile
import os
from io import BytesIO
from tempfile import TemporaryDirectory
from typing import Optional
from pathlib import Path
from datetime import datetime, timedelta
import hashlib

from .base_handler import BaseDataHandler
from ..utils.geocoding import CoordinateConverter, decimal_to_dms
from ..utils.logger import setup_logger
import logging

logger = setup_logger(__name__, log_file='logs/data_retrieval.log', level=logging.DEBUG)


class TaipeiHandler(BaseDataHandler):
    """
    Handler for Taipei City handicapped parking data.

    Data source: Taipei City Open Data Platform
    Format: ZIP file containing shapefiles
    Filtering: pktype == "03" for handicapped parking
    """

    def __init__(self, config, source_id=None):
        """Initialize handler with cache configuration."""
        super().__init__(config, source_id)
        self.cache_dir = Path('cache/taipei_city')
        self.cache_expiry_days = 7
        self.debug_dir = Path('debug')

    def _get_cache_path(self) -> Path:
        """Get the cache file path for the shapefile."""
        # Create a hash of the URL to use as filename
        url_str = self.url if self.url else "default"
        url_hash = hashlib.md5(url_str.encode()).hexdigest()
        return self.cache_dir / f'shapefile_{url_hash}.zip'

    def _is_cache_valid(self, cache_path: Path) -> bool:
        """Check if cached file exists and is not expired."""
        if not cache_path.exists():
            logger.info(f"Cache file does not exist: {cache_path}")
            return False

        # Check file age
        file_time = datetime.fromtimestamp(cache_path.stat().st_mtime)
        age = datetime.now() - file_time
        expiry = timedelta(days=self.cache_expiry_days)

        if age > expiry:
            logger.info(f"Cache expired (age: {age.days} days, expiry: {self.cache_expiry_days} days)")
            return False

        logger.info(f"Cache is valid (age: {age.days} days)")
        return True

    def _save_to_cache(self, content: bytes, cache_path: Path):
        """Save downloaded content to cache."""
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_bytes(content)
        logger.info(f"Saved {len(content)} bytes to cache: {cache_path}")

    def _save_debug_csv(self, data, stage: str):
        """
        Save GeoDataFrame or DataFrame to CSV for debugging.

        Args:
            data: GeoDataFrame or DataFrame to save
            stage: Stage name (e.g., 'raw_data', 'filtered_data', 'transformed_data')
        """
        try:
            self.debug_dir.mkdir(parents=True, exist_ok=True)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            csv_path = self.debug_dir / f'taipei_{stage}_{timestamp}.csv'

            # Create a copy for export (convert geometry to WKT string)
            debug_df = data.copy()
            if 'geometry' in debug_df.columns:
                debug_df['geometry_wkt'] = debug_df['geometry'].apply(lambda g: g.wkt if g is not None else None)
                debug_df['geometry_type'] = debug_df['geometry'].apply(lambda g: g.geom_type if g is not None else None)
                debug_df['centroid_x'] = debug_df['geometry'].apply(lambda g: g.centroid.x if g is not None else None)
                debug_df['centroid_y'] = debug_df['geometry'].apply(lambda g: g.centroid.y if g is not None else None)
                debug_df = debug_df.drop(columns=['geometry'])

            debug_df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            logger.info(f"Saved debug CSV ({len(debug_df)} records) to: {csv_path}")
        except Exception as e:
            logger.warning(f"Failed to save debug CSV: {e}")

    def fetch_data(self) -> gpd.GeoDataFrame:
        """
        Fetch and filter shapefile data from Taipei City open data.
        Uses caching to avoid repeated downloads.

        Returns:
            gpd.GeoDataFrame: Filtered geodataframe with handicapped parking records

        Raises:
            requests.HTTPError: If download fails
            FileNotFoundError: If no shapefile found in ZIP
            Exception: If data processing fails
        """
        logger.info("Starting Taipei City data retrieval")
        logger.info(f"Source URL: {self.url}")

        # Check cache first
        cache_path = self._get_cache_path()
        zip_content = None

        if self._is_cache_valid(cache_path):
            logger.info(f"Using cached shapefile from: {cache_path}")
            zip_content = cache_path.read_bytes()
        else:
            # Download ZIP file
            logger.info(f"Downloading from URL: {self.url}")
            try:
                response = requests.get(self.url, timeout=60)
                response.raise_for_status()
                zip_content = response.content
                logger.info(f"Downloaded {len(zip_content)} bytes")

                # Save to cache
                self._save_to_cache(zip_content, cache_path)
            except requests.RequestException as e:
                logger.error(f"Failed to download data: {e}")
                raise

        # Extract and read shapefile
        gdf = None
        with TemporaryDirectory() as tmpdir:
            try:
                with zipfile.ZipFile(BytesIO(zip_content)) as z:
                    logger.info(f"Extracting ZIP file with {len(z.namelist())} files")
                    z.extractall(tmpdir)

                    # Find .shp file
                    shp_files = [f for f in os.listdir(tmpdir) if f.endswith('.shp')]
                    if not shp_files:
                        raise FileNotFoundError("No .shp file found in ZIP archive")

                    shp_file = shp_files[0]
                    shp_path = os.path.join(tmpdir, shp_file)
                    logger.info(f"Reading shapefile: {shp_file}")

                    gdf = gpd.read_file(shp_path)
                    logger.info(f"Loaded {len(gdf)} records from shapefile")
                    logger.info(f"Shapefile columns: {list(gdf.columns)}")
                    logger.info(f"Shapefile CRS: {gdf.crs}")

                    # Save raw data to CSV for debugging
                    self._save_debug_csv(gdf, 'raw_data')

            except Exception as e:
                logger.error(f"Failed to extract/read shapefile: {e}")
                raise

        # Filter by pktype
        filter_field = self.config.get('filter_field', 'pktype')
        filter_value = self.config.get('filter_value', '03')
        logger.info(f"Filtering by {filter_field} == '{filter_value}'")

        if filter_field not in gdf.columns:
            logger.warning(f"Filter field '{filter_field}' not found in data. Available columns: {list(gdf.columns)}")
            # Try case-insensitive match
            matching_cols = [col for col in gdf.columns if col.lower() == filter_field.lower()]
            if matching_cols:
                filter_field = matching_cols[0]
                logger.info(f"Using case-insensitive match: {filter_field}")
            else:
                logger.error(f"Cannot find filter field {filter_field}")
                raise ValueError(f"Filter field '{filter_field}' not found in shapefile")

        # Filter the geodataframe
        initial_count = len(gdf)
        gdf = gdf[gdf[filter_field].astype(str) == str(filter_value)]
        logger.info(f"After filtering: {len(gdf)} handicapped parking records (from {initial_count} total)")

        if len(gdf) == 0:
            logger.warning("No records match filter criteria!")
        else:
            # Save filtered data to CSV for debugging
            self._save_debug_csv(gdf, 'filtered_data')

        return gdf

    def transform_data(self, gdf: gpd.GeoDataFrame) -> pd.DataFrame:
        """
        Transform geodataframe to standard format with coordinate conversion.

        Args:
            gdf: GeoDataFrame from fetch_data()

        Returns:
            pd.DataFrame: Transformed data with standard columns
        """
        logger.info(f"Starting transformation for {len(gdf)} records")
        data = []
        errors = 0

        # Detect coordinate system
        crs = gdf.crs
        is_wgs84 = False

        if crs is not None:
            # Check if CRS is WGS84 (EPSG:4326)
            if crs.to_epsg() == 4326:
                is_wgs84 = True
                logger.info(f"Detected WGS84 coordinate system (EPSG:4326)")
            elif crs.to_epsg() == 3826:
                is_wgs84 = False
                logger.info(f"Detected TWD97 coordinate system (EPSG:3826)")
            else:
                logger.info(f"Detected CRS: {crs} (EPSG:{crs.to_epsg()})")
                # Try to auto-detect based on coordinate ranges
                sample_geom = gdf.iloc[0].geometry.centroid
                sample_x, sample_y = sample_geom.x, sample_geom.y
                if 100000 <= sample_x <= 400000 and 2400000 <= sample_y <= 2900000:
                    is_wgs84 = False
                    logger.info(f"Auto-detected TWD97 based on coordinate ranges (x={sample_x}, y={sample_y})")
                elif -180 <= sample_x <= 180 and -90 <= sample_y <= 90:
                    is_wgs84 = True
                    logger.info(f"Auto-detected WGS84 based on coordinate ranges (x={sample_x}, y={sample_y})")
                else:
                    logger.warning(f"Could not auto-detect coordinate system. Assuming TWD97.")
                    is_wgs84 = False
        else:
            logger.warning("No CRS information in shapefile. Will attempt auto-detection.")
            # Auto-detect based on first valid geometry
            sample_geom = gdf.iloc[0].geometry.centroid
            sample_x, sample_y = sample_geom.x, sample_geom.y
            if 100000 <= sample_x <= 400000 and 2400000 <= sample_y <= 2900000:
                is_wgs84 = False
                logger.info(f"Auto-detected TWD97 based on coordinate ranges (x={sample_x}, y={sample_y})")
            elif -180 <= sample_x <= 180 and -90 <= sample_y <= 90:
                is_wgs84 = True
                logger.info(f"Auto-detected WGS84 based on coordinate ranges (x={sample_x}, y={sample_y})")
            else:
                logger.warning(f"Could not auto-detect coordinate system. Assuming TWD97.")
                is_wgs84 = False

        row_count = 0
        for idx, row in gdf.iterrows():
            try:
                # Extract coordinates from geometry using centroid
                # (All Taipei City parking data are Polygon type)
                geom = row.geometry

                if geom is None or geom.is_empty:
                    logger.debug(f"Row {idx}: Empty or null geometry, skipping")
                    continue

                # Always use centroid for consistent point representation
                centroid = geom.centroid
                x, y = centroid.x, centroid.y

                if row_count < 5:  # Log first few for debugging
                    logger.debug(f"Row {idx}: Geometry type={geom.geom_type}, centroid=({x}, {y})")

                row_count += 1

                # Convert coordinates based on detected system
                if is_wgs84:
                    # Already in WGS84, use directly
                    lat, lon = y, x  # Note: in WGS84, x=lon, y=lat
                    logger.debug(f"Row {idx}: Using WGS84 coordinates directly (lat={lat}, lon={lon})")
                else:
                    # Convert from TWD97 to WGS84
                    lat, lon = CoordinateConverter.twd97_to_wgs84(x, y)
                    logger.debug(f"Row {idx}: Converted TWD97 ({x}, {y}) to WGS84 ({lat}, {lon})")

                # Generate DMS format
                dms_lat = decimal_to_dms(lat, is_latitude=True)
                dms_long = decimal_to_dms(lon, is_latitude=False)

                # Map fields using helper method
                city = self._get_field_value(row, 'city', 'Taipei City')
                area = self._get_field_value(row, 'area', '')
                road = self._get_field_value(row, 'road', '')

                # Clean up field values and handle invalid strings
                area = str(area).strip()
                road = str(road).strip()

                # Replace invalid values with empty string
                if area in ['None', 'nan', '<NA>', 'NaN', '']:
                    area = ''
                if road in ['None', 'nan', '<NA>', 'NaN', '']:
                    road = ''

                data.append({
                    'city': city,
                    'area': area,
                    'road': road,
                    'dd_lat': lat,
                    'dd_long': lon,
                    'dms_lat': dms_lat,
                    'dms_long': dms_long
                })

            except Exception as e:
                errors += 1
                logger.error(f"Error transforming row {idx}: {e}")
                continue

        logger.info(f"Transformation complete: {len(data)} valid records, {errors} errors")

        if len(data) == 0:
            logger.warning("No valid records after transformation!")
            return pd.DataFrame(data)

        # Create DataFrame
        result_df = pd.DataFrame(data)

        # Save transformed data to CSV for debugging
        self._save_debug_csv(result_df, 'transformed_data')

        return result_df

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

from .base_handler import BaseDataHandler
from ..utils.geocoding import CoordinateConverter, decimal_to_dms
from ..utils.logger import setup_logger

logger = setup_logger(__name__, log_file='logs/data_retrieval.log')


class TaipeiHandler(BaseDataHandler):
    """
    Handler for Taipei City handicapped parking data.

    Data source: Taipei City Open Data Platform
    Format: ZIP file containing shapefiles
    Filtering: pktype == "03" for handicapped parking
    """

    def fetch_data(self) -> gpd.GeoDataFrame:
        """
        Fetch and filter shapefile data from Taipei City open data.

        Returns:
            gpd.GeoDataFrame: Filtered geodataframe with handicapped parking records

        Raises:
            requests.HTTPError: If download fails
            FileNotFoundError: If no shapefile found in ZIP
            Exception: If data processing fails
        """
        logger.info("Starting Taipei City data retrieval")
        logger.info(f"Downloading from URL: {self.url}")

        # Download ZIP file
        try:
            response = requests.get(self.url, timeout=60)
            response.raise_for_status()
            logger.info(f"Downloaded {len(response.content)} bytes")
        except requests.RequestException as e:
            logger.error(f"Failed to download data: {e}")
            raise

        # Extract and read shapefile
        gdf = None
        with TemporaryDirectory() as tmpdir:
            try:
                with zipfile.ZipFile(BytesIO(response.content)) as z:
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

        for idx, row in gdf.iterrows():
            try:
                # Extract coordinates from geometry
                geom = row.geometry

                # Handle different geometry types
                if geom.geom_type == 'Point':
                    x, y = geom.x, geom.y
                elif geom.geom_type in ['Polygon', 'MultiPolygon']:
                    centroid = geom.centroid
                    x, y = centroid.x, centroid.y
                    logger.debug(f"Row {idx}: Using centroid of {geom.geom_type}")
                else:
                    logger.warning(f"Row {idx}: Unsupported geometry type {geom.geom_type}, skipping")
                    continue

                # Convert TWD97 to WGS84
                lat, lon = CoordinateConverter.twd97_to_wgs84(x, y)

                # Generate DMS format
                dms_lat = decimal_to_dms(lat, is_latitude=True)
                dms_long = decimal_to_dms(lon, is_latitude=False)

                # Map fields using helper method
                city = self._get_field_value(row, 'city', 'Taipei City')
                area = self._get_field_value(row, 'area', '')
                road = self._get_field_value(row, 'road', '')

                # Clean up field values
                area = str(area).strip() if area else ''
                road = str(road).strip() if road else ''

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

"""
New Taipei City handicapped parking data handler.
Fetches paginated data (JSON or CSV), filters by parking type, and converts to standard format.
"""

import requests
import pandas as pd
from io import StringIO
from typing import List, Dict, Any
from pathlib import Path
from datetime import datetime, timedelta
import hashlib
import pickle

from .base_handler import BaseDataHandler
from ..utils.geocoding import CoordinateConverter, decimal_to_dms
from ..utils.logger import setup_logger
import logging

logger = setup_logger(__name__, log_file='logs/data_retrieval.log', level=logging.DEBUG)


class NewTaipeiHandler(BaseDataHandler):
    """
    Handler for New Taipei City handicapped parking data.

    Data source: New Taipei City Open Data API
    Format: Paginated JSON or CSV API
    Filtering: charged field contains "身汽" (handicapped vehicle parking)
    """

    def __init__(self, config, source_id=None):
        """Initialize handler with cache and debug configuration."""
        super().__init__(config, source_id)
        self.cache_dir = Path('cache/new_taipei_city')
        self.cache_expiry_days = 7
        self.debug_dir = Path('debug')

    def _save_debug_csv(self, data, stage: str):
        """
        Save DataFrame to CSV for debugging.

        Args:
            data: DataFrame to save
            stage: Stage name (e.g., 'raw_data', 'filtered_data', 'transformed_data')
        """
        try:
            self.debug_dir.mkdir(parents=True, exist_ok=True)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            csv_path = self.debug_dir / f'new_taipei_{stage}_{timestamp}.csv'

            # Create a copy for export
            debug_df = data.copy()
            debug_df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            logger.info(f"Saved debug CSV ({len(debug_df)} records) to: {csv_path}")
        except Exception as e:
            logger.warning(f"Failed to save debug CSV: {e}")

    def _get_cache_path(self) -> Path:
        """Get the cache file path for the API data."""
        # Create a hash of the URL to use as filename
        url_str = self.url if self.url else "default"
        url_hash = hashlib.md5(url_str.encode()).hexdigest()
        return self.cache_dir / f'api_data_{url_hash}.pkl'

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

    def _save_to_cache(self, dataframe: pd.DataFrame, cache_path: Path):
        """Save DataFrame to cache using pickle."""
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        with open(cache_path, 'wb') as f:
            pickle.dump(dataframe, f)
        logger.info(f"Saved {len(dataframe)} records to cache: {cache_path}")

    def _load_from_cache(self, cache_path: Path) -> pd.DataFrame:
        """Load DataFrame from cache."""
        with open(cache_path, 'rb') as f:
            dataframe = pickle.load(f)
        logger.info(f"Loaded {len(dataframe)} records from cache: {cache_path}")
        return dataframe

    # Area code to Chinese district name mapping
    AREA_CODE_MAPPING = {
        '65000010': '板橋區',
        '65000020': '三重區',
        '65000030': '中和區',
        '65000040': '永和區',
        '65000050': '新莊區',
        '65000060': '新店區',
        '65000070': '樹林區',
        '65000080': '鶯歌區',
        '65000090': '三峽區',
        '65000100': '淡水區',
        '65000110': '汐止區',
        '65000120': '瑞芳區',
        '65000130': '土城區',
        '65000140': '蘆洲區',
        '65000150': '五股區',
        '65000160': '泰山區',
        '65000170': '林口區',
        '65000180': '深坑區',
        '65000190': '石碇區',
        '65000200': '坪林區',
        '65000210': '三芝區',
        '65000220': '石門區',
        '65000230': '八里區',
        '65000240': '平溪區',
        '65000250': '雙溪區',
        '65000260': '貢寮區',
        '65000270': '金山區',
        '65000280': '萬里區',
        '65000290': '烏來區',
    }

    def fetch_data(self) -> pd.DataFrame:
        """
        Fetch and filter paginated data from New Taipei City API.

        Supports both JSON and CSV formats based on config.

        Returns:
            pd.DataFrame: Filtered dataframe with handicapped parking records

        Raises:
            requests.HTTPError: If API request fails
            Exception: If data processing fails
        """
        logger.info("Starting New Taipei City data retrieval")

        # Check cache first
        cache_path = self._get_cache_path()
        if self._is_cache_valid(cache_path):
            logger.info(f"Using cached data from: {cache_path}")
            return self._load_from_cache(cache_path)

        # Check format
        data_format = self.config.get('format', 'json_paginated')
        is_csv = 'csv' in data_format.lower()

        logger.info(f"Data format: {data_format}")

        all_data = []
        page = 0
        page_size = self.config.get('page_size', 1000)
        filter_field = self.config.get('filter_field', 'charged')
        filter_pattern = self.config.get('filter_pattern', '身汽')

        logger.info(f"Fetching data with filter: {filter_field} contains '{filter_pattern}'")
        logger.info(f"Page size: {page_size}")

        max_pages = 100  # Safety limit to prevent infinite loops
        while page < max_pages:
            try:
                # Make paginated request
                url = f"{self.url}?page={page}&size={page_size}"
                logger.info(f"Fetching page {page} from: {url}")

                response = requests.get(url, timeout=30)
                response.raise_for_status()

                # Parse based on format
                if is_csv:
                    # Parse CSV response
                    csv_content = response.text
                    if not csv_content.strip():
                        logger.info(f"Empty CSV response at page {page}, stopping pagination")
                        break

                    df_page = pd.read_csv(StringIO(csv_content))
                    logger.info(f"Page {page}: received {len(df_page)} records")

                    # Check if data is empty
                    if len(df_page) == 0:
                        logger.info(f"No more data at page {page}, stopping pagination")
                        break

                    # Filter by charged field containing pattern
                    if filter_field in df_page.columns:
                        df_filtered = df_page[df_page[filter_field].astype(str).str.contains(filter_pattern, na=False)]
                        logger.info(f"Page {page}: {len(df_filtered)} records match filter")
                        all_data.append(df_filtered)
                    else:
                        logger.warning(f"Filter field '{filter_field}' not found in CSV columns: {list(df_page.columns)}")
                        all_data.append(df_page)

                    # If we got less than page_size records, we're at the end
                    if len(df_page) < page_size:
                        logger.info(f"Received fewer records than page size, stopping pagination")
                        break

                else:
                    # Parse JSON response
                    data = response.json()

                    # Check if response is a list or dict
                    if isinstance(data, dict):
                        # Some APIs wrap the data in a dict like {"data": [...]}
                        if 'data' in data:
                            data = data['data']
                        elif 'records' in data:
                            data = data['records']
                        elif 'results' in data:
                            data = data['results']

                    # Ensure data is a list
                    if not isinstance(data, list):
                        logger.error(f"Unexpected API response format: {type(data)}")
                        break

                    logger.info(f"Page {page}: received {len(data)} records")

                    # Check if data is empty (end of pagination)
                    if not data or len(data) == 0:
                        logger.info(f"No more data at page {page}, stopping pagination")
                        break

                    # Filter by charged field containing pattern
                    filtered_data = [
                        record for record in data
                        if filter_pattern in str(record.get(filter_field, ''))
                    ]
                    logger.info(f"Page {page}: {len(filtered_data)} records match filter")

                    all_data.append(pd.DataFrame(filtered_data))

                    # If we got less than page_size records, we're probably at the end
                    if len(data) < page_size:
                        logger.info(f"Received fewer records than page size, stopping pagination")
                        break

                page += 1

            except requests.RequestException as e:
                logger.error(f"Error fetching page {page}: {e}")
                # If first page fails, raise error; otherwise continue with what we have
                if page == 0:
                    raise
                else:
                    logger.warning(f"Stopping pagination due to error on page {page}")
                    break

            except Exception as e:
                logger.error(f"Unexpected error on page {page}: {e}")
                break

        # Combine all data
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            logger.info(f"Total records fetched: {len(combined_df)} (from {page} pages)")

            # Save to cache
            self._save_to_cache(combined_df, cache_path)

            # Save filtered data to CSV for debugging
            self._save_debug_csv(combined_df, 'filtered_data')
        else:
            logger.warning("No records retrieved from API!")
            combined_df = pd.DataFrame()

        return combined_df

    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform dataframe to standard format with coordinate conversion.

        Args:
            df: DataFrame from fetch_data()

        Returns:
            pd.DataFrame: Transformed data with standard columns
        """
        logger.info(f"Starting transformation for {len(df)} records")
        data = []
        errors = 0

        # Determine if coordinates are already in WGS84 or need conversion from TWD97
        is_wgs84 = self.coordinate_system == 'WGS84'

        if is_wgs84:
            # Get WGS84 coordinate fields
            lat_field = self.fields_mapping.get('lat', 'lat')
            lon_field = self.fields_mapping.get('lon', 'lon')
            logger.debug(f"Using WGS84 coordinate fields: lat={lat_field}, lon={lon_field}")
        else:
            # Get TWD97 coordinate fields
            x_field = self.fields_mapping.get('x', 'X')
            y_field = self.fields_mapping.get('y', 'Y')
            logger.debug(f"Using TWD97 coordinate fields: X={x_field}, Y={y_field}")

        for idx, row in df.iterrows():
            try:
                if is_wgs84:
                    # Extract WGS84 coordinates directly
                    lat = row.get(lat_field)
                    lon = row.get(lon_field)

                    # Validate coordinates
                    if pd.isna(lat) or pd.isna(lon):
                        logger.debug(f"Row {idx}: Missing coordinates, skipping")
                        continue

                    # Convert to float
                    try:
                        lat = float(lat)
                        lon = float(lon)
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Row {idx}: Invalid coordinate values lat={lat}, lon={lon}, skipping")
                        continue

                    # Validate coordinate ranges for WGS84
                    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
                        logger.warning(f"Row {idx}: Coordinates out of WGS84 range lat={lat}, lon={lon}, skipping")
                        continue

                else:
                    # Extract TWD97 coordinates
                    x = row.get(x_field)
                    y = row.get(y_field)

                    # Validate coordinates
                    if pd.isna(x) or pd.isna(y):
                        logger.debug(f"Row {idx}: Missing coordinates, skipping")
                        continue

                    # Convert to float
                    try:
                        x = float(x)
                        y = float(y)
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Row {idx}: Invalid coordinate values X={x}, Y={y}, skipping")
                        continue

                    # Validate coordinate ranges for TWD97
                    if not (100000 <= x <= 400000 and 2400000 <= y <= 2900000):
                        logger.warning(f"Row {idx}: Coordinates out of TWD97 range X={x}, Y={y}, skipping")
                        continue

                    # Convert TWD97 to WGS84
                    lat, lon = CoordinateConverter.twd97_to_wgs84(x, y)

                # Generate DMS format
                dms_lat = decimal_to_dms(lat, is_latitude=True)
                dms_long = decimal_to_dms(lon, is_latitude=False)

                # Map fields using helper method
                city = self._get_field_value(row, 'city', 'New Taipei City')
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

                # Convert area code to Chinese district name
                if area in self.AREA_CODE_MAPPING:
                    area = self.AREA_CODE_MAPPING[area]
                    logger.debug(f"Row {idx}: Converted area code to district name: {area}")
                elif area:
                    logger.warning(f"Row {idx}: Unknown area code '{area}', keeping original value")

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

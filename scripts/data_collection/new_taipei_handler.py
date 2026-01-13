"""
New Taipei City handicapped parking data handler.
Fetches paginated data (JSON or CSV), filters by parking type, and converts to standard format.
"""

import requests
import pandas as pd
from io import StringIO
from typing import List, Dict, Any

from .base_handler import BaseDataHandler
from ..utils.geocoding import CoordinateConverter, decimal_to_dms
from ..utils.logger import setup_logger

logger = setup_logger(__name__, log_file='logs/data_retrieval.log')


class NewTaipeiHandler(BaseDataHandler):
    """
    Handler for New Taipei City handicapped parking data.

    Data source: New Taipei City Open Data API
    Format: Paginated JSON or CSV API
    Filtering: charged field contains "身汽" (handicapped vehicle parking)
    """

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

        # Get field mappings
        x_field = self.fields_mapping.get('x', 'X')
        y_field = self.fields_mapping.get('y', 'Y')

        logger.debug(f"Using coordinate fields: X={x_field}, Y={y_field}")

        for idx, row in df.iterrows():
            try:
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

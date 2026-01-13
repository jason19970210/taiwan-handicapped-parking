"""
Abstract base class for data source handlers.
Defines the interface that all city handlers must implement.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any
import pandas as pd


class BaseDataHandler(ABC):
    """
    Abstract base class for data source handlers.

    Each city data source should implement this interface
    to provide consistent data processing.
    """

    def __init__(self, config: Dict[str, Any], source_id: str = None):
        """
        Initialize the handler with configuration.

        Args:
            config: Configuration dictionary containing:
                - url: Source URL
                - format: Data format
                - coordinate_system: Coordinate system (TWD97, WGS84)
                - fields_mapping: Field name mappings
                - other source-specific configurations
            source_id: Source identifier (e.g., 'taipei_city', 'new_taipei_city')
        """
        self.config = config
        self.url = config.get('url')
        self.format = config.get('format')
        self.coordinate_system = config.get('coordinate_system', 'WGS84')
        self.fields_mapping = config.get('fields_mapping', {})
        self.source_id = source_id

    @abstractmethod
    def fetch_data(self) -> pd.DataFrame:
        """
        Fetch raw data from the source.

        This method should:
        1. Download/fetch data from the source URL
        2. Parse the data into a DataFrame
        3. Apply any source-specific filtering
        4. Return raw data ready for transformation

        Returns:
            pd.DataFrame: Raw data from source

        Raises:
            Exception: If data fetching fails
        """
        pass

    @abstractmethod
    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform raw data to standard format.

        This method should:
        1. Extract/map fields according to fields_mapping
        2. Convert coordinates to WGS84 if needed
        3. Generate DMS format coordinates
        4. Return standardized DataFrame

        Args:
            df: Raw DataFrame from fetch_data()

        Returns:
            pd.DataFrame: Transformed data with standard columns:
                - city: str
                - area: str
                - road: str
                - dd_lat: float (decimal degrees latitude)
                - dd_long: float (decimal degrees longitude)
                - dms_lat: str (DMS format latitude)
                - dms_long: str (DMS format longitude)

        Raises:
            Exception: If transformation fails
        """
        pass

    def process(self) -> pd.DataFrame:
        """
        Main processing pipeline: fetch and transform data.

        This method orchestrates the complete data processing workflow.

        Returns:
            pd.DataFrame: Processed data in standard format

        Raises:
            Exception: If any step in the pipeline fails
        """
        raw_data = self.fetch_data()
        transformed_data = self.transform_data(raw_data)

        return transformed_data

    def _get_field_value(self, row: pd.Series, mapping_key: str, default: str = '') -> str:
        """
        Helper method to get field value from row using mapping.

        Handles both direct field mapping and "fixed:" values.

        Args:
            row: DataFrame row
            mapping_key: Key in fields_mapping
            default: Default value if field not found

        Returns:
            str: Field value

        Examples:
            >>> # For fixed value: "fixed:Taipei City"
            >>> handler._get_field_value(row, 'city')
            'Taipei City'

            >>> # For mapped field: "district" -> row['district']
            >>> handler._get_field_value(row, 'area')
            'Zhongshan District'
        """
        mapping_value = self.fields_mapping.get(mapping_key, default)

        if isinstance(mapping_value, str) and mapping_value.startswith('fixed:'):
            # Fixed value like "fixed:Taipei City"
            return mapping_value.replace('fixed:', '')
        else:
            # Field mapping like "district" -> row['district']
            return str(row.get(mapping_value, default))

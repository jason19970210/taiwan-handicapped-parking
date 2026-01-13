"""
Data merger for combining multiple data sources.
Handles dynamic handler loading, data merging, and deduplication.
"""

import pandas as pd
import importlib
from typing import Dict, List, Any

from ..utils.logger import setup_logger

logger = setup_logger(__name__, log_file='logs/data_retrieval.log')


class DataMerger:
    """
    Merges data from multiple sources with deduplication.

    Dynamically loads handlers based on configuration and
    combines their data into a single deduplicated DataFrame.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize merger with configuration.

        Args:
            config: Configuration dictionary containing:
                - sources: List of data source configurations
                - output: Output configuration with deduplication keys
        """
        self.config = config
        self.sources = config.get('sources', [])
        self.output_config = config.get('output', {})
        self.dedup_keys = self.output_config.get('deduplication_keys', [])

    def collect_and_merge(self) -> pd.DataFrame:
        """
        Collect data from all sources and merge with deduplication.

        Returns:
            pd.DataFrame: Merged and deduplicated data

        Raises:
            Exception: If any handler fails or no data is collected
        """
        logger.info(f"Starting data collection from {len(self.sources)} sources")
        all_data = []
        successful_sources = 0
        failed_sources = 0

        for source in self.sources:
            source_id = source.get('id', 'unknown')
            source_name = source.get('name', source_id)

            # Check if source is enabled
            if not source.get('enabled', True):
                logger.info(f"Source '{source_name}' is disabled, skipping")
                continue

            try:
                logger.info(f"Processing source: {source_name}")

                # Dynamically load handler
                handler = self._load_handler(source)

                # Process data
                df = handler.process()

                if df is not None and len(df) > 0:
                    all_data.append(df)
                    logger.info(f"Successfully collected {len(df)} records from {source_name}")
                    successful_sources += 1
                else:
                    logger.warning(f"No data collected from {source_name}")

            except Exception as e:
                failed_sources += 1
                logger.error(f"Failed to process source '{source_name}': {e}", exc_info=True)
                # Continue with other sources even if one fails
                continue

        logger.info(f"Collection complete: {successful_sources} successful, {failed_sources} failed")

        # Check if we have any data
        if not all_data:
            error_msg = "No data collected from any source"
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Merge all dataframes
        logger.info(f"Merging {len(all_data)} dataframes")
        merged = pd.concat(all_data, ignore_index=True)
        logger.info(f"Total records before deduplication: {len(merged)}")

        # Deduplicate
        if self.dedup_keys:
            logger.info(f"Deduplicating using keys: {self.dedup_keys}")
            initial_count = len(merged)
            merged = merged.drop_duplicates(subset=self.dedup_keys, keep='first')
            duplicates_removed = initial_count - len(merged)
            logger.info(f"Removed {duplicates_removed} duplicate records")
            logger.info(f"Final record count: {len(merged)}")
        else:
            logger.warning("No deduplication keys specified, skipping deduplication")

        # Sort by city and area for consistent output
        if 'city' in merged.columns and 'area' in merged.columns:
            merged = merged.sort_values(['city', 'area', 'road'], ignore_index=True)
            logger.info("Sorted records by city, area, and road")

        return merged

    def _load_handler(self, source: Dict[str, Any]):
        """
        Dynamically load and instantiate a data handler.

        Args:
            source: Source configuration dictionary

        Returns:
            BaseDataHandler: Instantiated handler

        Raises:
            ImportError: If handler module cannot be imported
            AttributeError: If handler class cannot be found
        """
        handler_name = source.get('handler')
        if not handler_name:
            raise ValueError(f"No handler specified for source {source.get('id')}")

        try:
            # Convert handler name to module path
            # e.g., "taipei_handler" -> "scripts.data_collection.taipei_handler"
            module_path = f"scripts.data_collection.{handler_name}"
            logger.debug(f"Importing module: {module_path}")

            module = importlib.import_module(module_path)

            # Convert handler name to class name
            # e.g., "taipei_handler" -> "TaipeiHandler"
            class_name = ''.join(word.capitalize() for word in handler_name.split('_'))
            logger.debug(f"Loading class: {class_name}")

            HandlerClass = getattr(module, class_name)

            # Instantiate handler with source config
            handler = HandlerClass(source.get('config', {}))
            logger.debug(f"Successfully loaded handler: {class_name}")

            return handler

        except ImportError as e:
            logger.error(f"Cannot import handler module '{handler_name}': {e}")
            raise
        except AttributeError as e:
            logger.error(f"Cannot find handler class in module '{handler_name}': {e}")
            raise
        except Exception as e:
            logger.error(f"Error loading handler '{handler_name}': {e}")
            raise

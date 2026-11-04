"""
New Taipei City handicapped parking data handler.
Fetches paginated data (JSON or CSV), filters by parking type, and converts to standard format.
"""

import requests
import pandas as pd
from io import StringIO
from typing import List, Dict, Any, Optional
from pathlib import Path
from datetime import datetime, timedelta
import hashlib
import pickle
import time

from .base_handler import BaseDataHandler
from ..utils.geocoding import CoordinateConverter, decimal_to_dms
from ..utils.logger import setup_logger
import logging

logger = setup_logger(__name__, log_file='logs/data_retrieval.log', level=logging.DEBUG)


class PaginationError(RuntimeError):
    """The paginated API could not be read completely and consistently."""


class NewTaipeiHandler(BaseDataHandler):
    """
    Handler for New Taipei City handicapped parking data.

    Data source: New Taipei City Open Data API
    Format: Paginated JSON or CSV API
    Filtering: name field contains "汽車身心障礙專用" (handicapped car space)
    """

    def __init__(self, config, source_id=None):
        """Initialize handler with cache and debug configuration."""
        super().__init__(config, source_id)
        # The live API serves lower-case column names (it used to be upper case).
        # Match case-insensitively so a cosmetic rename can never again silently
        # disable the filter and the area/road mapping.
        self.fields_mapping = {
            k: (v if (not isinstance(v, str) or v.startswith('fixed:')) else v.lower())
            for k, v in self.fields_mapping.items()
        }
        self.filter_field = str(config.get('filter_field', 'name')).lower()
        self.filter_pattern = config.get('filter_pattern', '汽車身心障礙專用')
        self.max_retries = int(config.get('max_retries', 6))
        self.retry_backoff = float(config.get('retry_backoff_seconds', 2.0))
        self.retry_backoff_max = float(config.get('retry_backoff_max_seconds', 30.0))
        self.request_timeout = float(config.get('request_timeout_seconds', 60))
        self.connect_timeout = float(config.get('connect_timeout_seconds', 10))
        # Whole-crawl limits, shared by every nested retry/confirmation loop:
        #   crawl_deadline_seconds  monotonic wall-clock cap for fetch_data (default 600 s;
        #                           a healthy crawl takes ~25 s)
        #   retry_budget            total retries (failed reads + glitch re-reads) across the
        #                           crawl (default 60)
        self.crawl_deadline = float(config.get('crawl_deadline_seconds', 600))
        self.retry_budget = int(config.get('retry_budget', 60))
        self.end_confirmations = int(config.get('end_confirmations', 2))
        self.max_pages = int(config.get('max_pages', 200))
        self.required_columns = sorted(
            {self.filter_field}
            | {
                v
                for k, v in self.fields_mapping.items()
                if k in ('area', 'road', 'lat', 'lon') and not v.startswith('fixed:')
            }
        )
        # Columns that identify a row; live status fields (e.g. parkingstatus) are excluded
        # so a legitimate status flip between two reads is not mistaken for a different page.
        self.identity_columns = [
            c.lower() for c in config.get('identity_columns', [])
        ] or sorted(set(self.required_columns) | {'id', 'cellid'})
        self._now = time.monotonic
        self._start_crawl()
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
        # v2: v1 caches may hold the unfiltered ~31k-row result of the column-case bug
        return self.cache_dir / f'api_data_v2_{url_hash}.pkl'

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

    def _start_crawl(self):
        """Reset the shared deadline and retry budget (called at the start of every crawl)."""
        self._deadline = self._now() + self.crawl_deadline
        self._retries_left = self.retry_budget

    def _remaining(self) -> float:
        return self._deadline - self._now()

    def _check_deadline(self):
        if self._remaining() <= 0:
            raise PaginationError(
                f"crawl deadline of {self.crawl_deadline:g}s exceeded; refusing to continue"
            )

    def _spend_retry(self, what: str):
        """Take one retry from the budget shared by all loops; fail loudly when it is gone."""
        self._retries_left -= 1
        if self._retries_left < 0:
            raise PaginationError(
                f"shared retry budget of {self.retry_budget} exhausted ({what})"
            )

    def _sleep_backoff(self, attempt: int):
        self._check_deadline()
        time.sleep(min(self.retry_backoff * (2 ** (attempt - 1)), self.retry_backoff_max,
                       max(self._remaining(), 0)))

    def _identity_digest(self, df: pd.DataFrame) -> str:
        """Order-independent hash of the identity columns of a page."""
        cols = [c for c in self.identity_columns if c in df.columns]
        rows = sorted(tuple(map(str, r)) for r in df[cols].itertuples(index=False, name=None))
        return hashlib.sha256(repr(rows).encode('utf-8')).hexdigest()

    def _read_page(self, page: int, page_size: int) -> pd.DataFrame:
        """
        Request one page and parse it. Retries with exponential backoff on
        network errors, HTTP errors, unparsable bodies and bodies that do not
        have the expected columns. Every retry is charged to the crawl-wide
        retry budget and every request is checked against the crawl deadline.
        Raises PaginationError once any limit runs out.

        Returns an empty DataFrame for an empty body (a legitimate "past the
        end" answer, which callers must confirm). Column names are lower-cased.
        """
        url = f"{self.url}?page={page}&size={page_size}"
        last_error = None
        for attempt in range(1, self.max_retries + 1):
            self._check_deadline()
            try:
                logger.info(f"Fetching page {page} (attempt {attempt}/{self.max_retries}): {url}")
                remaining = self._remaining()
                response = requests.get(
                    url,
                    timeout=(min(self.connect_timeout, remaining),
                             max(1, min(self.request_timeout, remaining))),
                )
                self._check_deadline()  # a slow request that finally answered is still too late
                response.raise_for_status()
                text = response.text
                if not text.strip():
                    return pd.DataFrame()
                df = pd.read_csv(StringIO(text), dtype=str)
                df.columns = [str(c).strip().lower() for c in df.columns]
                missing = [c for c in self.required_columns if c not in df.columns]
                if missing:
                    raise ValueError(
                        f"response is missing columns {missing}; got {list(df.columns)[:20]}"
                    )
                return df
            except (requests.RequestException, ValueError, pd.errors.ParserError,
                    pd.errors.EmptyDataError) as e:
                last_error = e
                self._check_deadline()  # a slow failure past the deadline ends the crawl now
                logger.warning(f"Page {page} attempt {attempt}/{self.max_retries} failed: {e!r}")
                self._spend_retry(f"page {page}: {e!r}")
                if attempt < self.max_retries:
                    self._sleep_backoff(attempt)
        raise PaginationError(
            f"page {page} failed after {self.max_retries} attempts: {last_error!r}"
        ) from last_error

    def _confirm_last_page(self, page: int, page_size: int, df: pd.DataFrame) -> bool:
        """
        Is the short page `df` really the last page? True only if, on
        `end_confirmations` spaced-out checks, page+1 is empty AND re-reading
        `page` returns the same rows (compared by identity-column hash, not
        just the row count). Anything else counts as a glitch.
        """
        want = self._identity_digest(df)
        for check in range(1, self.end_confirmations + 1):
            self._sleep_backoff(check)
            if len(self._read_page(page + 1, page_size)) > 0:
                return False
            if self._identity_digest(self._read_page(page, page_size)) != want:
                return False
        return True

    def _end_mismatch(self, total_rows: int, page_size: int, raw_pages) -> Optional[str]:
        """
        One independent check of the end of the data using a DIFFERENT page size, so a
        server that consistently truncates the last page at one offset geometry
        cannot pass. The alt-size page that contains offset `total_rows` must hold
        exactly the rows from there up to `total_rows`, with the same identities as
        the tail we already hold. Returns None when it does, else a description.

        When `total_rows` is an exact multiple of the alt size that page is expected
        to be empty, and an empty reply is also what an all-empty outage looks like.
        So the end is never accepted on emptiness alone: the last FULL alt-size page
        (the one just before) must also come back with exactly `alt` rows whose
        identity digest equals the digest of the last `alt` collected rows. The digest
        is order-independent (sorted), so it is set/multiset equality and does not
        rely on both page sizes ordering rows identically.
        """
        alt = page_size - max(1, page_size // 10)
        if alt < 1:
            alt = page_size + 1
        alt_page = total_rows // alt
        expected = total_rows - alt_page * alt
        tail = pd.concat(raw_pages, ignore_index=True) if raw_pages else pd.DataFrame()
        want = self._identity_digest(tail.iloc[len(tail) - expected:]) if expected else None
        got = self._read_page(alt_page, alt)
        if len(got) != expected or (expected and self._identity_digest(got) != want):
            return (f"page {alt_page} at size {alt} got {len(got)} rows, expected {expected}"
                    + ("" if len(got) != expected else " with different content"))
        if expected == 0:
            # Positive evidence: no non-empty reply has confirmed the tail yet.
            if total_rows <= 0 or len(tail) < alt:
                return f"no collected tail of {alt} rows to confirm the end (total {total_rows})"
            last_full = alt_page - 1
            full = self._read_page(last_full, alt)
            if len(full) != alt:
                return (f"last full page {last_full} at size {alt} got {len(full)} rows, "
                        f"expected {alt}")
            if self._identity_digest(full) != self._identity_digest(tail.iloc[len(tail) - alt:]):
                return (f"last full page {last_full} at size {alt} has rows that differ "
                        f"from the collected tail")
        logger.info(
            f"End cross-check OK: page {alt_page} at size {alt} holds the expected "
            f"{expected} trailing rows (total {total_rows})"
        )
        return None

    def _verify_end(self, total_rows: int, page_size: int, raw_pages) -> None:
        """Run _end_mismatch up to max_retries times; raise PaginationError if it never agrees."""
        last = "no read"
        for attempt in range(1, self.max_retries + 1):
            last = self._end_mismatch(total_rows, page_size, raw_pages)
            if last is None:
                return
            logger.warning(f"End cross-check mismatch ({last}); re-reading ({attempt}/{self.max_retries})")
            self._spend_retry(f"end cross-check: {last}")
            self._sleep_backoff(attempt)
        raise PaginationError(
            f"end cross-check failed after {self.max_retries} attempts: {last}; "
            f"the last page of {total_rows} rows looks truncated or the data moved"
        )

    def _fetch_page(self, page: int, page_size: int, end_check=None):
        """
        Fetch one page and decide whether it is the last one.

        The API gives no total count and, in practice, answers some requests
        with an empty body or a truncated page while still saying HTTP 200
        (seen live: 14 consecutive empty pages, and 881/823-row pages). So:
        - a full page is never the last page;
        - an empty first page is never accepted. A later empty page (reached only
          after a full page) is the end only if a spaced re-read of the same
          offset is empty again; fetch_data() then requires the end cross-check
          at another page size to pass. Anything else is retried with backoff;
        - a short non-empty page is the last page only after
          _confirm_last_page() agrees.

        Returns (DataFrame, is_last). Raises PaginationError if the page cannot
        be obtained consistently within max_retries attempts.
        """
        reason = "no attempt made"
        for attempt in range(1, self.max_retries + 1):
            df = self._read_page(page, page_size)
            n = len(df)
            if n == page_size:
                return df, False
            if n == 0:
                reason = "empty body"
                if page > 0:  # the previous page was full, or we would not be here
                    self._sleep_backoff(1)
                    if len(self._read_page(page, page_size)) > 0:
                        reason = "empty body that returned rows on re-read"
                    else:
                        mismatch = end_check() if end_check else "no end cross-check supplied"
                        if mismatch is None:
                            return df, True
                        reason = f"empty body that the end cross-check rejects ({mismatch})"
            elif self._confirm_last_page(page, page_size, df):
                return df, True
            else:
                reason = f"short page ({n} < {page_size}) that is not confirmed as the last"
            logger.warning(
                f"Page {page}: {reason}; treating as a glitch and re-reading "
                f"(attempt {attempt}/{self.max_retries})"
            )
            self._spend_retry(f"page {page}: {reason}")
            self._sleep_backoff(attempt)
        raise PaginationError(
            f"page {page} did not return a consistent full page after "
            f"{self.max_retries} attempts (last: {reason})"
        )

    def fetch_data(self) -> pd.DataFrame:
        """
        Fetch and filter paginated CSV data from the New Taipei City API.

        Fails loudly (PaginationError) instead of returning a partial result:
        a page that cannot be fetched after retries, a missing filter column,
        or a run with no matching rows at all. Nothing is cached unless the
        whole crawl succeeded.

        Returns:
            pd.DataFrame: Filtered dataframe with handicapped parking records
                (lower-case column names)
        """
        logger.info("Starting New Taipei City data retrieval")

        cache_path = self._get_cache_path()
        if self._is_cache_valid(cache_path):
            logger.info(f"Using cached data from: {cache_path}")
            return self._load_from_cache(cache_path)

        page_size = int(self.config.get('page_size', 1000))
        logger.info(
            f"Fetching with filter: {self.filter_field} contains '{self.filter_pattern}', "
            f"page size {page_size}"
        )

        self._start_crawl()
        frames = []
        raw_pages = []  # last two raw pages, for the end cross-check
        total_rows = 0
        page = 0
        while True:
            if page >= self.max_pages:
                raise PaginationError(
                    f"no end of data after {self.max_pages} pages; refusing to guess"
                )
            df_page, is_last = self._fetch_page(
                page, page_size,
                end_check=lambda: self._end_mismatch(total_rows, page_size, raw_pages),
            )
            total_rows += len(df_page)
            if len(df_page) > 0:
                raw_pages = (raw_pages + [df_page])[-2:]
            if len(df_page) > 0:
                matched = df_page[
                    df_page[self.filter_field].astype(str).str.contains(
                        self.filter_pattern, regex=False, na=False
                    )
                ]
                logger.info(
                    f"Page {page}: {len(df_page)} records, {len(matched)} match filter"
                )
                frames.append(matched)
            if is_last:
                break
            page += 1

        self._verify_end(total_rows, page_size, raw_pages)

        if not frames or sum(len(f) for f in frames) == 0:
            raise PaginationError(
                f"{total_rows} rows fetched over {page + 1} pages but none match "
                f"{self.filter_field} contains '{self.filter_pattern}'"
            )

        combined_df = pd.concat(frames, ignore_index=True)
        logger.info(
            f"Total: {total_rows} rows fetched in {page + 1} pages, "
            f"{len(combined_df)} match the filter"
        )

        self._save_to_cache(combined_df, cache_path)
        self._save_debug_csv(combined_df, 'filtered_data')
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

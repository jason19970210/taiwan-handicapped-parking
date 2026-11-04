# Taiwan Handicapped Parking Data Management

Automated system for collecting, managing, and syncing handicapped parking location data from Taiwan cities to Google Maps.

## Features

- **Automated Data Collection**: Fetches handicapped parking data from Taipei City and New Taipei City open data platforms
- **Smart Caching**: 1-week cache for API data to reduce bandwidth and improve performance
- **Smart Filtering**: Filters locations by parking type (handicapped vehicle parking)
- **Automatic Coordinate System Detection**: Auto-detects WGS84 or TWD97 from shapefile CRS or coordinate ranges
- **Polygon Geometry Support**: Extracts centroid coordinates from polygon parking zones
- **Flexible Coordinate Handling**: Supports both TWD97 (with conversion) and WGS84 (direct use) coordinate systems
- **Coordinate Conversion**: Converts TWD97 coordinates to WGS84 and DMS formats using pyproj
- **Data Validation**: Validates coordinate fields while allowing optional area/road information
- **Debug Mode**: Saves intermediate data to CSV files for troubleshooting
- **Google Maps Integration**: Syncs the per-district lists to Google Maps saved lists from a dedicated host with a logged-in Chrome profile (no credentials in the repo or in CI)
- **GitHub Actions Automation**: Scheduled weekly updates with manual trigger support
- **Comprehensive Logging**: Detailed logs for data retrieval and Playwright operations

## Data Sources

### Current Coverage
- **Taipei City**:
  - Format: Shapefile data (ZIP archive) from Taipei City Open Data Platform
  - Geometry: Polygon zones (centroid used for point coordinates)
  - Filtering: `pktype == "03"` for handicapped parking
  - Coordinates: Auto-detected (WGS84 or TWD97)
  - Caching: 1-week cache for ZIP files

- **New Taipei City**:
  - Format: CSV API with pagination from New Taipei City Open Data
  - Filtering: `name` field contains "汽車身心障礙專用" (column names are matched case-insensitively; a missing filter column is a hard error, never "keep everything")
  - Coordinates: WGS84 (lat/lon fields used directly)
  - Page size: 1000 records per request
  - The API sometimes answers HTTP 200 with an empty body or a truncated page mid-run. Pages are retried with backoff; an empty page is never taken as the end of data, a short page only after it is re-read and the next page is confirmed empty; if a page still cannot be read the run fails and writes nothing. The last page is re-checked by content (identity columns) and against the same offset read at another page size. A crawl-wide deadline (600 s) and retry budget (60) bound all nested retries.
  - Caching: 1-week cache for API responses, written only after a complete crawl

### Data Format
CSV file with columns:
- `city`: City name (required)
- `area`: District/area (optional, may be empty)
- `road`: Street/road name (optional, may be empty)
- `dd_lat`: Decimal degrees latitude in WGS84 (required)
- `dd_long`: Decimal degrees longitude in WGS84 (required)
- `dms_lat`: DMS format latitude (required, e.g., `25°01'58.80"N`)
- `dms_long`: DMS format longitude (required, e.g., `121°33'55.44"E`)

**Note**: Area and road fields may be empty as source data sometimes has missing values. Only coordinate fields (dd_lat, dd_long, dms_lat, dms_long) are validated for completeness.

## Setup

### Prerequisites
- Python 3.11+
- For the Google Maps sync only: a host with Google Chrome and a Chrome profile logged in by hand (see `deploy/ubuntu/README.md`)

### Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/taiwan-handicapped-parking.git
cd taiwan-handicapped-parking
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```
No Playwright browser download is needed: the Maps sync drives the installed Google Chrome (`channel="chrome"`).

## Usage

### Command Line Interface

The system provides three main commands:

#### 1. Collect Data
```bash
python scripts/main.py collect
```
- Downloads data from all configured sources
- Filters and transforms data
- Merges and deduplicates records
- Any failing source fails the whole run (no partial CSV)
- Count guard: per-city row counts are compared with the CSV on disk before it is overwritten. A city that loses more than 5% (`--max-drop-pct`, or `output.max_drop_pct` in the config) or gains more than 50% (`--max-growth-pct`) aborts the run with exit code 2 and writes nothing. Use `--accept-count-change` only for an intended dataset change.
- Saves to `data/parking_locations.csv`

#### Per-district lists (no Google writes)
```bash
python scripts/main.py build-lists          # data/lists/*.csv + index.json + assignment_report.json
python scripts/main.py plan-sync --out tmp/sync_plan.json   # add/remove plan vs data/sync_state.json
```
- District of every row comes from point-in-polygon against `data/boundaries/tw_districts_tpe_ntpc.geojson` (MOI/NLSC township boundaries, data.gov.tw dataset 7441; source URL, version and simplification are recorded inside the file; rebuild with `scripts/build_boundaries.py`).
- One list per city + district; a district over 500 rows is split into numbered lists (`-1`, `-2`, ...) of at most 500 rows, cut from a stable sort by road, latitude, longitude. Rows are never merged.
- `data/sync_state.json` records what was last written to Google per list, keyed by point id (`TPE:`/`NTPC:` + lat,lon at 6 decimals; same-coordinate rows get a short road hash). An empty state makes `plan-sync` print the full initial load.

#### 2. Sync the lists to Google Maps (ubuntu host)
```bash
python scripts/main.py sync-lists --dry-run                      # counts only, no browser
python scripts/main.py sync-lists --only "臺北市 中山區" --max-ops 230
python scripts/main.py sync-lists --count "臺北市 中山區"          # places Google shows vs. the state file
python scripts/main.py sync-lists                                # daily run: up to --max-ops 300 ops
```
- Uses a Chrome profile that was logged in by hand (`--profile`, default `~/.config/thp-maps-profile`). The profile is the only credential; no email or password exists anywhere, and the command never fills a login form.
- Plans add/remove ops per list from `data/lists` against its own state file (`--state`, default `~/.local/state/thp-maps/sync_state.json`, not `data/sync_state.json`), applies them one place at a time with 6-12 s pauses, and writes the state after every verified op, so a stopped run resumes where it ended.
- Touches only the lists named in `data/lists/index.json`; never deletes a list.
- One JSON line per op in `sync.log` next to the state file. Exit codes: 0 done / nothing to do, 1 error, 2 partial (ops remain), 3 relogin needed or blocked, 4 three consecutive failures.
- `authenticate` and `sync-maps` (the old password flow) are retired and exit 1.
- Install, timer and relogin procedure: `deploy/ubuntu/README.md`.

### GitHub Actions Automation

The workflow `update_parking_data.yml` runs every Monday at 9am UTC+8 (and on manual trigger). It collects data, rebuilds the per-district lists (`build-lists`; a failure fails the job and commits nothing), checks for changes in the CSV and `data/lists`, and, if anything changed, commits both to a temporary branch and merges to main. It holds no Google secret and does not talk to Google; the Maps sync runs on the ubuntu host.

## Project Structure

```
taiwan-handicapped-parking/
├── .github/workflows/
│   └── update_parking_data.yml      # GitHub Actions workflow
├── data/
│   ├── parking_locations.csv        # Main data file (tracked)
│   └── data_sources.json            # Source configuration
├── scripts/
│   ├── main.py                      # CLI entrypoint
│   ├── data_collection/
│   │   ├── base_handler.py          # Abstract handler
│   │   ├── taipei_handler.py        # Taipei City handler
│   │   ├── new_taipei_handler.py    # New Taipei City handler
│   │   └── merger.py                # Data merger
│   ├── utils/
│   │   ├── geocoding.py             # Coordinate conversion
│   │   ├── csv_validator.py        # Data validation
│   │   └── logger.py                # Logging utilities
│   ├── lists/
│   │   ├── builder.py               # Per-district lists
│   │   ├── sync_plan.py             # Add/remove plan vs. state
│   │   └── maps_sync.py             # Google Maps saved-list sync (ubuntu)
│   └── google_maps/
│       └── selectors.py             # Every Google Maps selector, in one place
├── deploy/ubuntu/                   # systemd user timer + install/relogin README
├── tests/                           # Test files
│   ├── test_geocoding.py            # Geocoding tests
│   └── test_handlers.py             # Handler tests
├── cache/                           # Cache directory (ignored)
│   ├── taipei_city/                 # Taipei City cache
│   └── new_taipei_city/             # New Taipei City cache
├── debug/                           # Debug CSV exports (ignored)
├── logs/                            # Log files (ignored)
│   ├── data_retrieval.log
│   ├── playwright.log
│   └── app.log
├── requirements.txt
└── README.md
```

## Configuration

### Adding New Data Sources

Edit `data/data_sources.json`:

```json
{
  "sources": [
    {
      "id": "new_city",
      "name": "New City Handicapped Parking",
      "enabled": true,
      "handler": "new_city_handler",
      "config": {
        "url": "https://data.city.gov.tw/api/...",
        "format": "json_paginated",
        "coordinate_system": "TWD97",
        "filter_field": "parking_type",
        "filter_pattern": "handicapped",
        "fields_mapping": {
          "city": "fixed:New City",
          "area": "district",
          "road": "address",
          "x": "X",
          "y": "Y"
        }
      }
    }
  ]
}
```

Then create a handler in `scripts/data_collection/new_city_handler.py` implementing `BaseDataHandler`.

## Technical Details

### Coordinate System Detection

The system automatically detects coordinate systems for shapefile data:

1. **CRS Metadata**: Checks shapefile CRS information
   - EPSG:4326 → WGS84 (coordinates used directly)
   - EPSG:3826 → TWD97 (coordinates converted to WGS84)

2. **Auto-detection by Range**: When CRS is unknown or non-standard
   - TWD97 range: X ∈ [100000, 400000], Y ∈ [2400000, 2900000]
   - WGS84 range: X ∈ [-180, 180], Y ∈ [-90, 90]

3. **Fallback**: Assumes TWD97 if unable to detect

### Polygon Geometry Handling

Taipei City parking data uses polygon geometries representing parking zones:

- **Centroid Extraction**: System extracts the geometric center (centroid) of each polygon
- **Consistent Representation**: All locations represented as point coordinates for Google Maps
- **Geometry Types Supported**: Point, Polygon, MultiPolygon

### Coordinate Conversion

The system uses [pyproj](https://pyproj4.github.io/pyproj/) for accurate coordinate transformations:

- **TWD97 to WGS84**: Converts Taiwan Datum 1997 (EPSG:3826) to World Geodetic System 1984 (EPSG:4326)
- **Consistent Ordering**: Uses `always_xy=True` for predictable coordinate ordering
  - TWD97: (x, y) = (easting, northing)
  - WGS84: (x, y) = (longitude, latitude)
- **DMS Conversion**: Generates degrees-minutes-seconds format from decimal degrees
- **Round-trip Accuracy**: Maintains sub-meter precision in coordinate transformations

### Data Caching

Both handlers implement 1-week caching to optimize performance:

- **Taipei City**: Caches downloaded ZIP files containing shapefiles
- **New Taipei City**: Caches paginated API responses
- **Cache Location**: `cache/<city_name>/` directories
- **Expiry**: 7 days from last fetch
- **Benefits**: Reduces bandwidth, speeds up repeated runs, respects API rate limits

### Debug Mode

Handlers save intermediate data for troubleshooting:

- **Raw Data**: Original data from source before filtering
- **Filtered Data**: Data after applying filter criteria
- **Transformed Data**: Final standardized output
- **Location**: `debug/` directory with timestamped filenames
- **Format**: CSV with UTF-8-sig encoding for Excel compatibility

## Data Validation

The system validates CSV output using Pydantic models:

### Validation Rules

- **Required Fields**: city, dd_lat, dd_long, dms_lat, dms_long
- **Optional Fields**: area, road (may be empty)
- **Coordinate Validation**:
  - Latitude: -90 to 90 degrees
  - Longitude: -180 to 180 degrees
  - Taiwan bounds: Lat 21.5-25.5°N, Lon 119.5-122.5°E
- **DMS Format**: Must contain °, ', " symbols and N/S/E/W direction

### Validation Process

1. Check for required columns
2. Validate coordinate fields for missing values
3. Check coordinate ranges (global and Taiwan-specific)
4. Verify DMS format strings
5. Detect duplicate records
6. Generate validation report with errors and warnings

## Logging

The system generates three log files:

- `logs/data_retrieval.log`: Data collection operations
  - Source downloads, filtering, coordinate conversions
  - Record counts and statistics
  - Cache hits and misses
  - Errors during data processing

- `logs/playwright.log`: legacy log of the retired password flow (no longer written)

- `logs/app.log`: General application logs
  - Main script execution
  - Configuration loading
  - Overall workflow status
  - Validation results

## Troubleshooting

### Authentication Issues

`sync-lists` exits 3 when the Chrome profile is signed out or Google shows a captcha or consent page. Redo the manual login described in `deploy/ubuntu/README.md` (relogin procedure).

### Data Collection Fails

1. **Check network**: Ensure data source URLs are accessible
2. **Clear cache**: Delete `cache/` directory to force fresh download
3. **Check debug output**: Review CSV files in `debug/` directory to inspect intermediate data
4. **Verify field names**: Data sources may change field names - check logs and update `fields_mapping`
5. **Check coordinates**: Ensure coordinate values are in valid ranges
6. **Review logs**: Check `logs/data_retrieval.log` for specific errors

### Cache Issues

1. **Stale data**: Delete `cache/<city_name>/` to force fresh fetch
2. **Cache corruption**: Remove cache files and re-run collection
3. **Disk space**: Ensure sufficient space for cache files (typically < 10MB per city)

### Google Maps Sync Issues

1. **Selectors changed**: Google may update its UI; a failing op logs "... not found (NAME)" in `sync.log`. Fix that constant in `scripts/google_maps/selectors.py`
2. **Blocked**: exit 3 with reason "blocked" (captcha, "unusual traffic", consent page). Stop for the day and retry later; do not raise the pace
3. **Count mismatch**: compare `sync-lists --count "<list>"` with the state file; Google may merge two points that resolve to the same place

## Development

### Running Tests

The project includes comprehensive test coverage:

```bash
# Run all tests
pytest tests/ -v

# Run specific test suite
pytest tests/test_geocoding.py -v

# Run with coverage report
pytest tests/ --cov=scripts --cov-report=html

# Run only coordinate conversion tests
pytest tests/test_geocoding.py::TestCoordinateConverter -v
```

**Test Coverage**:
- `test_geocoding.py`: Coordinate conversion, DMS format, validation, Taiwan bounds
- `test_handlers.py`: Data handler implementations (if implemented)

**Note**: Coordinate conversion tests require `pyproj` to be installed. The tests validate:
- Decimal to DMS conversion accuracy
- TWD97 ↔ WGS84 coordinate transformations
- Round-trip conversion precision (< 10 meters)
- Taiwan bounds checking

### Local Development

1. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install in development mode:
```bash
pip install -r requirements.txt
pip install -e .
```

3. Test data collection:
```bash
python scripts/main.py collect
```

4. Preview the Google Maps sync (no browser, writes nothing):
```bash
python scripts/main.py sync-lists --dry-run --state /tmp/thp_state.json
```

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is open source and available under the MIT License.

## Acknowledgments

- Taipei City Open Data Platform
- New Taipei City Open Data Platform
- Playwright for browser automation
- GeoPandas for spatial data processing

## Contact

For questions or issues, please open an issue on GitHub.

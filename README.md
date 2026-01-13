# Taiwan Handicapped Parking Data Management

Automated system for collecting, managing, and syncing handicapped parking location data from Taiwan cities to Google Maps.

## Features

- **Automated Data Collection**: Fetches handicapped parking data from Taipei City and New Taipei City open data platforms
- **Smart Filtering**: Filters locations by parking type (handicapped vehicle parking)
- **Coordinate Conversion**: Converts TWD97 coordinates to WGS84 and DMS formats
- **Google Maps Integration**: Automatically syncs locations to a Google Maps saved list
- **GitHub Actions Automation**: Scheduled weekly updates with manual trigger support
- **Comprehensive Logging**: Detailed logs for data retrieval and Playwright operations

## Data Sources

### Current Coverage
- **Taipei City**: Shapefile data from Taipei City Open Data Platform
- **New Taipei City**: CSV API with pagination from New Taipei City Open Data

### Data Format
CSV file with columns:
- `city`: City name
- `area`: District/area
- `road`: Street/road name
- `dd_lat`: Decimal degrees latitude (WGS84)
- `dd_long`: Decimal degrees longitude (WGS84)
- `dms_lat`: DMS format latitude (e.g., `25°01'58.80"N`)
- `dms_long`: DMS format longitude (e.g., `121°33'55.44"E`)

## Setup

### Prerequisites
- Python 3.11+
- Google account for Maps integration

### Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/taiwan-handicapped-parking.git
cd taiwan-handicapped-parking
```

2. Install dependencies:
```bash
pip install -r requirements.txt
playwright install chromium
```

3. Configure environment variables (for local testing):
```bash
export GOOGLE_EMAIL="your@email.com"
export GOOGLE_PASSWORD="yourpassword"  # Or app password if 2FA enabled
export HEADLESS=false  # Set to true for headless mode
export RATE_LIMIT_DELAY=3  # Seconds between Google Maps requests
```

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
- Saves to `data/parking_locations.csv`

#### 2. Authenticate with Google Maps
```bash
python scripts/main.py authenticate
```
- Opens browser (headless or visible)
- Logs into Google Maps
- Saves authentication state for reuse

#### 3. Sync to Google Maps
```bash
python scripts/main.py sync-maps
```
- Loads parking locations from CSV
- Creates "Taiwan Handicapped Parking" list if needed
- Syncs each location to the list
- Skips locations already saved
- Respects rate limits to avoid detection

### GitHub Actions Automation

The workflow runs automatically:
- **Schedule**: Every Monday at 9am UTC+8
- **Manual Trigger**: Via GitHub Actions UI

#### Setup GitHub Secrets

Go to Repository Settings → Secrets → Actions and add:
- `GOOGLE_EMAIL`: Your Google account email
- `GOOGLE_PASSWORD`: Your Google account password (or app password)

#### Workflow Process
1. **Job 1 (collect-and-update)**:
   - Collects data from all sources
   - Checks for changes using `git diff`
   - If changed: commits to temporary branch, merges to main
   - Outputs: `data_changed` status

2. **Job 2 (sync-google-maps)**:
   - Runs only if Job 1 detected changes
   - Restores or creates Google auth state (cached)
   - Syncs all locations to Google Maps
   - Uploads logs as artifacts

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
│   └── google_maps/
│       ├── authenticator.py         # Google auth handler
│       ├── map_saver.py             # Location sync logic
│       └── selectors.py             # UI selectors
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

## Logging

The system generates three log files:

- `logs/data_retrieval.log`: Data collection operations
  - Source downloads, filtering, coordinate conversions
  - Record counts and statistics
  - Errors during data processing

- `logs/playwright.log`: Google Maps automation
  - Authentication steps
  - Location searches and saves
  - Rate limiting and progress

- `logs/app.log`: General application logs
  - Main script execution
  - Configuration loading
  - Overall workflow status

## Troubleshooting

### Authentication Issues

If Google authentication fails:

1. **Check credentials**: Verify `GOOGLE_EMAIL` and `GOOGLE_PASSWORD` are correct
2. **Use App Password**: If 2FA is enabled, create an app-specific password
3. **Clear auth state**: Delete `.github/auth/google_auth_state.json` and re-authenticate
4. **Check logs**: Review `logs/playwright.log` for detailed error messages

### Data Collection Fails

1. **Check network**: Ensure data source URLs are accessible
2. **Verify field names**: Data sources may change field names - check logs and update `fields_mapping`
3. **Check coordinates**: Ensure coordinate values are in valid ranges
4. **Review logs**: Check `logs/data_retrieval.log` for specific errors

### Google Maps Sync Issues

1. **Rate limiting**: Increase `RATE_LIMIT_DELAY` if encountering issues
2. **Selectors changed**: Google may update UI - adjust selectors in `selectors.py`
3. **Location not found**: Some addresses may not be searchable - check logs for warnings
4. **Bot detection**: If detected, increase delays and use stealth techniques

## Development

### Running Tests

```bash
# Run all tests
pytest tests/ -v

# Run specific test file
pytest tests/test_geocoding.py -v

# Run with coverage
pytest tests/ --cov=scripts --cov-report=html
```

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

4. Test with visible browser:
```bash
export HEADLESS=false
python scripts/main.py authenticate
python scripts/main.py sync-maps
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

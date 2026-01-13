"""
CSV validation utilities for parking location data.
Uses Pydantic for data validation.
"""

import pandas as pd
from pydantic import BaseModel, field_validator, ValidationError
from typing import List, Dict, Any


class ParkingLocation(BaseModel):
    """Pydantic model for parking location data validation."""

    city: str
    area: str
    road: str
    dd_lat: float
    dd_long: float
    dms_lat: str
    dms_long: str

    @field_validator('city', 'area', 'road')
    @classmethod
    def validate_not_empty(cls, v: str) -> str:
        """Validate that string fields are not empty."""
        if not v or not v.strip():
            raise ValueError('Field cannot be empty')
        return v.strip()

    @field_validator('dd_lat')
    @classmethod
    def validate_latitude(cls, v: float) -> float:
        """Validate latitude is within valid range."""
        if not -90 <= v <= 90:
            raise ValueError(f'Latitude must be between -90 and 90, got {v}')
        # Taiwan specific validation (optional)
        if not 21.5 <= v <= 25.5:
            raise ValueError(f'Latitude {v} is outside Taiwan bounds (21.5 to 25.5)')
        return v

    @field_validator('dd_long')
    @classmethod
    def validate_longitude(cls, v: float) -> float:
        """Validate longitude is within valid range."""
        if not -180 <= v <= 180:
            raise ValueError(f'Longitude must be between -180 and 180, got {v}')
        # Taiwan specific validation (optional)
        if not 119.5 <= v <= 122.5:
            raise ValueError(f'Longitude {v} is outside Taiwan bounds (119.5 to 122.5)')
        return v

    @field_validator('dms_lat', 'dms_long')
    @classmethod
    def validate_dms_format(cls, v: str) -> str:
        """Validate DMS format string."""
        if not v or not v.strip():
            raise ValueError('DMS field cannot be empty')

        # Check for required symbols: °, ', "
        if '°' not in v or "'" not in v or '"' not in v:
            raise ValueError(f'Invalid DMS format: {v}. Must contain °, \', and "')

        # Check for direction (N, S, E, W)
        if not any(d in v for d in ['N', 'S', 'E', 'W']):
            raise ValueError(f'Invalid DMS format: {v}. Must end with N, S, E, or W')

        return v.strip()


class ValidationResult:
    """Container for validation results."""

    def __init__(self):
        self.errors: List[str] = []
        self.warnings: List[str] = []
        self.valid_rows: int = 0
        self.invalid_rows: int = 0

    def add_error(self, error: str):
        """Add an error message."""
        self.errors.append(error)
        self.invalid_rows += 1

    def add_warning(self, warning: str):
        """Add a warning message."""
        self.warnings.append(warning)

    def is_valid(self) -> bool:
        """Check if validation passed (no errors)."""
        return len(self.errors) == 0

    def summary(self) -> str:
        """Get validation summary."""
        return (
            f"Validation Summary:\n"
            f"  Valid rows: {self.valid_rows}\n"
            f"  Invalid rows: {self.invalid_rows}\n"
            f"  Errors: {len(self.errors)}\n"
            f"  Warnings: {len(self.warnings)}"
        )


def validate_csv(df: pd.DataFrame) -> ValidationResult:
    """
    Validate parking locations CSV data.

    Args:
        df: DataFrame with parking location data

    Returns:
        ValidationResult with errors and warnings

    Examples:
        >>> df = pd.read_csv('data/parking_locations.csv')
        >>> result = validate_csv(df)
        >>> if not result.is_valid():
        ...     print(result.summary())
        ...     for error in result.errors:
        ...         print(f"  - {error}")
    """
    result = ValidationResult()

    # Check for required columns
    required_columns = ['city', 'area', 'road', 'dd_lat', 'dd_long', 'dms_lat', 'dms_long']
    missing_columns = set(required_columns) - set(df.columns)

    if missing_columns:
        result.add_error(f"Missing required columns: {', '.join(missing_columns)}")
        return result

    # Check for empty DataFrame
    if df.empty:
        result.add_warning("DataFrame is empty")
        return result

    # Validate each row
    for idx, row in df.iterrows():
        try:
            # Convert row to dict and validate
            row_dict = row.to_dict()
            ParkingLocation(**row_dict)
            result.valid_rows += 1

        except ValidationError as e:
            # Collect all validation errors for this row
            error_messages = []
            for error in e.errors():
                field = error['loc'][0]
                message = error['msg']
                error_messages.append(f"{field}: {message}")

            result.add_error(f"Row {idx}: {'; '.join(error_messages)}")

        except Exception as e:
            result.add_error(f"Row {idx}: Unexpected error - {str(e)}")

    # Check for duplicates
    duplicate_cols = ['city', 'area', 'road', 'dd_lat', 'dd_long']
    duplicates = df.duplicated(subset=duplicate_cols, keep=False)

    if duplicates.any():
        duplicate_count = duplicates.sum()
        result.add_warning(f"Found {duplicate_count} duplicate rows")

        # List duplicate rows
        duplicate_rows = df[duplicates].index.tolist()
        result.add_warning(f"Duplicate row indices: {duplicate_rows}")

    # Check for missing values
    for col in required_columns:
        missing = df[col].isna().sum()
        if missing > 0:
            result.add_error(f"Column '{col}' has {missing} missing values")

    return result


def validate_csv_file(file_path: str) -> ValidationResult:
    """
    Validate a CSV file.

    Args:
        file_path: Path to CSV file

    Returns:
        ValidationResult with errors and warnings

    Examples:
        >>> result = validate_csv_file('data/parking_locations.csv')
        >>> print(result.summary())
    """
    try:
        df = pd.read_csv(file_path, encoding='utf-8')
        return validate_csv(df)
    except FileNotFoundError:
        result = ValidationResult()
        result.add_error(f"File not found: {file_path}")
        return result
    except Exception as e:
        result = ValidationResult()
        result.add_error(f"Error reading CSV file: {str(e)}")
        return result


def remove_duplicates(df: pd.DataFrame, subset: List[str] = None) -> pd.DataFrame:
    """
    Remove duplicate rows from DataFrame.

    Args:
        df: DataFrame with parking location data
        subset: Columns to use for duplicate detection (default: city, area, road, dd_lat, dd_long)

    Returns:
        DataFrame with duplicates removed

    Examples:
        >>> df = pd.read_csv('data/parking_locations.csv')
        >>> df_clean = remove_duplicates(df)
    """
    if subset is None:
        subset = ['city', 'area', 'road', 'dd_lat', 'dd_long']

    return df.drop_duplicates(subset=subset, keep='first')


def fix_common_issues(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fix common data quality issues.

    Args:
        df: DataFrame with parking location data

    Returns:
        DataFrame with issues fixed

    Examples:
        >>> df = pd.read_csv('data/parking_locations.csv')
        >>> df_fixed = fix_common_issues(df)
    """
    df = df.copy()

    # Strip whitespace from string columns
    string_columns = ['city', 'area', 'road', 'dms_lat', 'dms_long']
    for col in string_columns:
        if col in df.columns:
            df[col] = df[col].str.strip()

    # Remove rows with any missing values
    df = df.dropna()

    # Remove duplicates
    df = remove_duplicates(df)

    return df

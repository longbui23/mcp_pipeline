import pandas as pd
from typing import Dict, Any, Optional, Tuple


def validate_schema(
    df: pd.DataFrame,
    expected_schema: Dict[str, Any]
) -> Tuple[bool, Dict[str, str]]:
    """
    Validate that DataFrame columns match expected schema.

    Args:
        df: Pandas DataFrame to validate
        expected_schema: Dict of column_name -> expected type (e.g., 'int64', 'float64', 'object')

    Returns:
        (is_valid, errors)
        is_valid: True if schema matches
        errors: dict of column -> error message if mismatch
    """
    errors = {}
    for col, expected_type in expected_schema.items():
        if col not in df.columns:
            errors[col] = "Missing column"
        elif str(df[col].dtype) != expected_type:
            errors[col] = f"Expected {expected_type}, got {df[col].dtype}"
    return len(errors) == 0, errors


def validate_nulls(
    df: pd.DataFrame,
    required_columns: Optional[list[str]] = None
) -> Tuple[bool, Dict[str, int]]:
    """
    Check required columns for null values.

    Args:
        df: Pandas DataFrame
        required_columns: List of columns that cannot have nulls. If None, checks all.

    Returns:
        (is_valid, null_counts)
        null_counts: dict of column -> number of nulls
    """
    if required_columns is None:
        required_columns = df.columns.tolist()
    null_counts = df[required_columns].isnull().sum().to_dict()
    errors = {col: count for col, count in null_counts.items() if count > 0}
    return len(errors) == 0, errors


def validate_min_max(
    df: pd.DataFrame,
    min_max_constraints: Optional[Dict[str, Tuple[Optional[float], Optional[float]]]] = None
) -> Tuple[bool, Dict[str, str]]:
    """
    Check numeric columns against min/max constraints.

    Args:
        df: Pandas DataFrame
        min_max_constraints: dict of column -> (min_value, max_value). Use None for no bound.

    Returns:
        (is_valid, errors)
    """
    if not min_max_constraints:
        return True, {}
    
    errors = {}
    for col, (min_val, max_val) in min_max_constraints.items():
        if col not in df.columns:
            errors[col] = "Column not found"
            continue
        if min_val is not None and (df[col] < min_val).any():
            errors[col] = f"Values below min {min_val}"
        if max_val is not None and (df[col] > max_val).any():
            if col in errors:
                errors[col] += f"; Values above max {max_val}"
            else:
                errors[col] = f"Values above max {max_val}"
    return len(errors) == 0, errors


def validate_dataframe(
    df: pd.DataFrame,
    expected_schema: Dict[str, Any],
    required_columns: Optional[list[str]] = None,
    min_max_constraints: Optional[Dict[str, Tuple[Optional[float], Optional[float]]]] = None
) -> Dict[str, Any]:
    """
    Run all validations and return a summary.

    Returns:
        dict with keys: 'schema_valid', 'nulls_valid', 'min_max_valid', 'errors'
    """
    schema_valid, schema_errors = validate_schema(df, expected_schema)
    nulls_valid, null_errors = validate_nulls(df, required_columns)
    min_max_valid, min_max_errors = validate_min_max(df, min_max_constraints)

    return {
        "schema_valid": schema_valid,
        "schema_errors": schema_errors,
        "nulls_valid": nulls_valid,
        "nulls_errors": null_errors,
        "min_max_valid": min_max_valid,
        "min_max_errors": min_max_errors,
        "overall_valid": schema_valid and nulls_valid and min_max_valid
    }


# -----------------------------
# Example usage
# -----------------------------
if __name__ == "__main__":
    data = {
        "id": [1, 2, 3, None],
        "age": [25, 30, 40, 50],
        "name": ["Alice", "Bob", "Charlie", "David"]
    }
    df = pd.DataFrame(data)

    expected_schema = {"id": "float64", "age": "int64", "name": "object"}
    required_columns = ["id", "name"]
    min_max_constraints = {"age": (18, 60)}

    result = validate_dataframe(df, expected_schema, required_columns, min_max_constraints)
    print(result)

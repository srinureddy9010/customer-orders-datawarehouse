"""
Data Quality Validation Module

This module provides data quality checks for the ETL pipeline.
Validates data integrity before transformation.
"""


def check_nulls(df, column):
    """
    Check for null values in a DataFrame column.
    
    Args:
        df: Pandas DataFrame
        column: Column name to validate
        
    Raises:
        ValueError: If null values are found
        
    Returns:
        bool: True if no nulls found
        
    Example:
        >>> check_nulls(customers, 'customer_id')
        True
    """
    null_count = df[column].isnull().sum()
    
    if null_count > 0:
        raise ValueError(
            f"❌ Data Quality Check Failed: {null_count} null values found in '{column}'"
        )
    
    print(f"✓ Null check passed: '{column}' has no null values")
    return True


def check_duplicates(df, column):
    """
    Check for duplicate values in a DataFrame column.
    
    Args:
        df: Pandas DataFrame
        column: Column name to validate
        
    Raises:
        ValueError: If duplicates are found
        
    Returns:
        bool: True if no duplicates found
        
    Example:
        >>> check_duplicates(products, 'product_id')
        True
    """
    duplicate_count = df[column].duplicated().sum()
    
    if duplicate_count > 0:
        raise ValueError(
            f"❌ Data Quality Check Failed: {duplicate_count} duplicate values found in '{column}'"
        )
    
    print(f"✓ Duplicate check passed: '{column}' has no duplicates")
    return True


def check_data_types(df, schema):
    """
    Validate data types in a DataFrame against expected schema.
    
    Args:
        df: Pandas DataFrame
        schema: Dict mapping column names to expected dtypes
        
    Raises:
        ValueError: If data types don't match
        
    Returns:
        bool: True if all data types match
        
    Example:
        >>> schema = {'customer_id': 'int64', 'name': 'object'}
        >>> check_data_types(customers, schema)
        True
    """
    for column, expected_dtype in schema.items():
        if column not in df.columns:
            raise ValueError(f"❌ Data Quality Check Failed: Column '{column}' not found")
        
        actual_dtype = str(df[column].dtype)
        
        if actual_dtype != expected_dtype:
            raise ValueError(
                f"❌ Data Quality Check Failed: Column '{column}' has type '{actual_dtype}' "
                f"but expected '{expected_dtype}'"
            )
    
    print(f"✓ Data type check passed: All columns match expected schema")
    return True


def check_value_range(df, column, min_val, max_val):
    """
    Check if values in a column are within expected range.
    
    Args:
        df: Pandas DataFrame
        column: Column name to validate
        min_val: Minimum acceptable value
        max_val: Maximum acceptable value
        
    Raises:
        ValueError: If values are outside range
        
    Returns:
        bool: True if all values are in range
        
    Example:
        >>> check_value_range(orders, 'amount', 0, 1000000)
        True
    """
    out_of_range = df[(df[column] < min_val) | (df[column] > max_val)]
    
    if len(out_of_range) > 0:
        raise ValueError(
            f"❌ Data Quality Check Failed: {len(out_of_range)} values in '{column}' "
            f"are outside range [{min_val}, {max_val}]"
        )
    
    print(f"✓ Range check passed: All values in '{column}' are within [{min_val}, {max_val}]")
    return True


def validate_all(data_sources):
    """
    Run all data quality checks on source data.
    
    Args:
        data_sources: Dict of DataFrames to validate
        
    Returns:
        bool: True if all checks pass
        
    Example:
        >>> data_sources = {
        ...     'customers': customers_df,
        ...     'products': products_df,
        ...     'orders': orders_df,
        ...     'payments': payments_df
        ... }
        >>> validate_all(data_sources)
        True
    """
    print("\n" + "=" * 60)
    print("RUNNING DATA QUALITY VALIDATION")
    print("=" * 60 + "\n")
    
    try:
        # Validate customers
        check_nulls(data_sources['customers'], 'customer_id')
        check_nulls(data_sources['customers'], 'name')
        check_duplicates(data_sources['customers'], 'customer_id')
        
        # Validate products
        check_nulls(data_sources['products'], 'product_id')
        check_nulls(data_sources['products'], 'product_name')
        check_duplicates(data_sources['products'], 'product_id')
        check_value_range(data_sources['products'], 'price', 0, 10000000)
        
        # Validate orders
        check_nulls(data_sources['orders'], 'order_id')
        check_nulls(data_sources['orders'], 'customer_id')
        check_duplicates(data_sources['orders'], 'order_id')
        check_value_range(data_sources['orders'], 'amount', 0, 100000000)
        
        # Validate payments
        check_nulls(data_sources['payments'], 'payment_id')
        check_duplicates(data_sources['payments'], 'payment_id')
        
        print("\n" + "✓ " * 30)
        print("✓ ALL DATA QUALITY CHECKS PASSED!")
        print("✓ " * 30 + "\n")
        
        return True
        
    except ValueError as e:
        print(f"\n{e}\n")
        return False

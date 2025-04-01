from common.prepare_functions import prepare_orders, prepare_products

# Test the prepare_orders function to ensure proper data type casting
def test_prepare_orders_from_file(spark, orders_df):
    # Apply the transformation
    result = prepare_orders(orders_df)

    # Define the expected data types after preparation
    expected_types = {
        'InvoiceNo': 'string',       # Should be string (e.g., invoice number like '536365')
        'StockCode': 'string',       # Product code as string
        'Quantity': 'int',           # Quantity should be an integer
        'InvoiceDate': 'timestamp',  # Should be parsed as a proper timestamp
        'CustomerID': 'string'       # Customer ID should be string, even if numeric-looking
    }

    # Assert that the schema matches exactly
    assert dict(result.dtypes) == expected_types

# Test the prepare_products function to ensure proper data type casting
def test_prepare_products_from_file(spark, products_df):
    # Apply the transformation
    result = prepare_products(products_df)

    # Define the expected schema after transformation
    expected_types = {
        'StockCode': 'string',      # Product code
        'Description': 'string',    # Product description
        'UnitPrice': 'double'       # Price should be a float/double
    }

    # Assert that the schema matches exactly
    assert dict(result.dtypes) == expected_types

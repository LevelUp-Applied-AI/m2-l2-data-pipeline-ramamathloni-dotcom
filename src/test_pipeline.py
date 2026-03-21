import pandas as pd
import pytest
from src.pipeline import load_data, clean_data, add_features

def test_load_data_returns_dataframe():
    """التأكد أن تحميل البيانات يعمل بشكل صحيح"""
    df = load_data('data/sales_records.csv')
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0
    expected_cols = ['date', 'store_id', 'product_category', 'quantity', 'unit_price', 'payment_method']
    for col in expected_cols:
        assert col in df.columns

def test_clean_data_no_nulls():
    """التأكد من عدم وجود قيم مفقودة بعد التنظيف"""
    df = load_data('data/sales_records.csv')
    cleaned = clean_data(df)
    assert cleaned['quantity'].isna().sum() == 0
    assert cleaned['unit_price'].isna().sum() == 0

def test_add_features_creates_revenue():
    """التأكد من حساب الأرباح بشكل صحيح"""
    df = load_data('data/sales_records.csv')
    cleaned = clean_data(df)
    enriched = add_features(cleaned)
    assert 'revenue' in enriched.columns
    expected_revenue = enriched['quantity'] * enriched['unit_price']
    pd.testing.assert_series_equal(enriched['revenue'], expected_revenue, check_names=False)
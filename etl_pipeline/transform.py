from pandas.api.types import is_numeric_dtype


def transform(df):
    if df is None or df.empty:
        raise ValueError("Input dataframe is empty. Nothing to transform.")

    transformed = df.copy()
    transformed = transformed.drop_duplicates()
    transformed.columns = [str(col).strip().lower() for col in transformed.columns]
    for column in transformed.columns:
        series = transformed[column]
        if is_numeric_dtype(series):
            median_value = series.median()
            fill_value = 0 if median_value != median_value else median_value
            transformed[column] = series.fillna(fill_value)
        else:
            transformed[column] = series.fillna("Unknown")
    return transformed
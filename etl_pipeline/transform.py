def transform(df):
    if df is None or df.empty:
        raise ValueError("Input dataframe is empty. Nothing to transform.")

    transformed = df.copy()
    transformed = transformed.drop_duplicates()
    transformed.columns = [str(col).strip().lower() for col in transformed.columns]
    transformed = transformed.fillna("Unknown")
    return transformed
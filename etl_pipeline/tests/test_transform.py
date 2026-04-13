import pandas as pd

from transform import transform


def test_transform_normalizes_columns_and_removes_duplicates():
    df = pd.DataFrame(
        [
            {"Name ": "Alice", "Age": 30},
            {"Name ": "Alice", "Age": 30},
            {"Name ": None, "Age": 40},
        ]
    )

    result = transform(df)

    assert list(result.columns) == ["name", "age"]
    assert len(result) == 2
    assert "Unknown" in result["name"].tolist()


def test_transform_raises_for_empty_dataframe():
    df = pd.DataFrame()
    try:
        transform(df)
    except ValueError as exc:
        assert "empty" in str(exc).lower()
        return
    assert False, "Expected ValueError for empty dataframe"

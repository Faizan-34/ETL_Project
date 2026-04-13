from pathlib import Path

from extract import extract


def test_extract_csv_reads_rows(tmp_path):
    csv_file = tmp_path / "sample.csv"
    csv_file.write_text("name,age\nAlice,30\nBob,40\n", encoding="utf-8")

    df = extract(str(csv_file))

    assert len(df) == 2
    assert list(df.columns) == ["name", "age"]


def test_extract_unsupported_format_raises(tmp_path):
    text_file = tmp_path / "notes.txt"
    text_file.write_text("hello", encoding="utf-8")

    try:
        extract(str(text_file))
    except ValueError as exc:
        assert "unsupported" in str(exc).lower()
        return
    assert False, "Expected ValueError for unsupported extension"


def test_extract_missing_file_raises():
    missing_file = Path("this_file_should_not_exist_123.csv")
    try:
        extract(str(missing_file))
    except FileNotFoundError:
        return
    assert False, "Expected FileNotFoundError for missing file"

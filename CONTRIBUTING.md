# Contributing

Thanks for your interest in contributing to the AER Data Pipeline.

## Getting Started

```bash
git clone https://github.com/QV2000/aer-data-pipeline.git
cd aer-data-pipeline
pip install -r requirements.txt
pip install pytest ruff  # dev dependencies
```

## Running Tests

```bash
pytest tests/ -v
```

All tests must pass before submitting a PR.

## Code Style

- Run `ruff check .` before committing. All ruff errors must be resolved.
- Use type hints on function signatures where practical.
- Write clear docstrings on all public functions.
- Never use em dashes in text. Use periods, commas, or restructure sentences instead.
- Follow existing patterns in the codebase.

## Adding a New Dataset

1. **Find the public source URL.** All data must come from a public government endpoint (AER, Petrinex, Saskatchewan GeoHub, etc.). No scraping private or paywalled sources.

2. **Add to `config/sources.yaml`.** Define the dataset with URL, cadence, format, parser reference, and schema:

```yaml
your_new_dataset:
    province: AB
    name: "Your Dataset Name"
    description: "What this dataset contains"
    source_type: direct_file  # or html_index, static_url, etc.
    url: "https://..."
    expected_cadence: monthly
    download_format: csv
    parser: parsers.your_parser.parse_function
    schema:
      required_columns:
        - column_a
        - column_b
      primary_keys: [column_a]
    validation:
      min_rows: 100
```

3. **Write a parser** in `parsers/` if the format is not already supported. Return a pandas DataFrame with normalized column names.

4. **Add a silver transform** in `transforms/silver.py` if the dataset needs custom cleaning beyond what the bronze layer provides.

5. **Add schema enforcement** in `config/schemas.py` with expected columns.

6. **Test it:**

```bash
python cli.py ingest --dataset your_new_dataset
python cli.py validate --dataset your_new_dataset
python cli.py build-duckdb
```

7. **Update the README** with your new dataset in the "What You Get" table.

8. **Add a query example** to `docs/query_cookbook.md`.

## Reporting Data Quality Issues

If you find incorrect data, missing columns, or parse errors:

1. Check if the source URL has changed by visiting the AER/Petrinex page directly.
2. Open an issue with: dataset name, expected vs. actual behavior, and the source URL.
3. If you can fix the parser, submit a PR with a test case that demonstrates the fix.

## Pull Request Process

1. Fork the repo and create a feature branch.
2. Make your changes.
3. Run `pytest tests/ -v` and `ruff check .`.
4. Submit a PR with a clear description of what changed and why.

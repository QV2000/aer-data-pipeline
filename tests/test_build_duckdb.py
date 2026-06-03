import duckdb

from build_duckdb import _create_analyst_views, _create_operator_views, _detect_wells_columns


def test_operator_groups_handles_wells_without_name_column():
    conn = duckdb.connect(":memory:")
    try:
        conn.execute("""
            CREATE TABLE wells (
                uwi VARCHAR,
                licensee VARCHAR,
                province VARCHAR,
                well_substance VARCHAR,
                well_cwi VARCHAR,
                well_purpose VARCHAR,
                well_completion_code VARCHAR,
                well_completion_type VARCHAR
            )
        """)
        conn.execute("""
            INSERT INTO wells VALUES
                ('00/01-01-001-01W2/0', 'ABC1', 'SK', 'OIL', 'SK123', 'PROD', 'HZ', 'DEV')
        """)

        assert _detect_wells_columns(conn) == ("licensee", None)

        _create_operator_views(conn)

        rows = conn.execute("""
            SELECT normalized_name, display_name, total_wells, ba_codes, ba_code_count
            FROM operator_groups
        """).fetchall()
        assert rows == [("ABC1", "ABC1", 1, "ABC1", 1)]
    finally:
        conn.close()


def test_analyst_views_handle_wells_without_name_column():
    conn = duckdb.connect(":memory:")
    try:
        conn.execute("""
            CREATE TABLE wells (
                uwi VARCHAR,
                licensee VARCHAR,
                province VARCHAR,
                well_substance VARCHAR
            )
        """)
        conn.execute("""
            INSERT INTO wells VALUES
                ('00/01-01-001-01W2/0', 'ABC1', 'SK', 'OIL')
        """)
        conn.execute("""
            CREATE TABLE production (
                uwi VARCHAR,
                oil_prod_vol DOUBLE,
                gas_prod_vol DOUBLE,
                water_prod_vol DOUBLE,
                productionmonth VARCHAR,
                province VARCHAR
            )
        """)
        conn.execute("""
            INSERT INTO production VALUES
                ('00/01-01-001-01W2/0', 10.0, 20.0, 5.0, '2026-04', 'SK')
        """)

        _create_analyst_views(conn)

        row = conn.execute("""
            SELECT well_name, licensee, fluid, mode, province, status_code, cumulative_oil_m3
            FROM v_well_summary
        """).fetchone()
        assert row == (None, "ABC1", "OIL", None, "SK", None, 10.0)

        scorecard = conn.execute("""
            SELECT operator_code, total_wells
            FROM v_operator_scorecard
        """).fetchall()
        assert scorecard == [("ABC1", 1)]
    finally:
        conn.close()

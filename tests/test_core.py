
from downloader.manifest import ManifestManager
from parsers.txt_parser import normalize_column_name
from transforms.silver import format_petrinex_uwi


def test_normalize_column_name():
    assert normalize_column_name("Facility ID") == "facility_id"
    assert normalize_column_name("UWI") == "uwi"
    assert normalize_column_name("ProductionMonth") == "production_month"
    assert normalize_column_name("  Well Total Depth  ") == "well_total_depth"


def test_normalize_column_name_special_chars():
    assert normalize_column_name("Well/Facility ID") == "well_facility_id"
    assert normalize_column_name("Operator (Name)") == "operator_name"
    assert normalize_column_name("some__double__underscore") == "some_double_underscore"


def test_format_petrinex_uwi_ab():
    assert format_petrinex_uwi("ABWI100010100308W402") == "00/01-01-003-08W4/2"


def test_format_petrinex_uwi_sk():
    assert format_petrinex_uwi("SKWI100010100308W202") == "00/01-01-003-08W2/2"


def test_format_petrinex_uwi_passthrough():
    assert format_petrinex_uwi("short") == "short"
    assert format_petrinex_uwi("00/01-01-003-08W4/2") == "00/01-01-003-08W4/2"


def test_manifest_round_trip(tmp_path):
    mm = ManifestManager(tmp_path, "test_ds")
    assert mm.has_changed(etag="abc") is True
    mm.create_entry("http://example.com", ["http://example.com"], b"data", etag="abc")
    assert mm.has_changed(etag="abc") is False
    assert mm.has_changed(etag="def") is True


def test_manifest_sha256_tracking(tmp_path):
    mm = ManifestManager(tmp_path, "test_ds")
    entry = mm.create_entry("http://example.com", ["http://example.com"], b"hello world")
    assert len(entry.sha256) == 64  # Full SHA256 hex digest
    assert entry.byte_size == 11


def test_manifest_failed_entry(tmp_path):
    mm = ManifestManager(tmp_path, "test_ds")
    entry = mm.create_failed_entry("http://example.com", [], "Connection timeout")
    assert entry.status == "failed"
    assert entry.error_message == "Connection timeout"


def test_manifest_persistence(tmp_path):
    mm1 = ManifestManager(tmp_path, "test_ds")
    mm1.create_entry("http://example.com", ["http://example.com"], b"data", etag="v1")

    # Load from disk in a new instance
    mm2 = ManifestManager(tmp_path, "test_ds")
    assert mm2.has_changed(etag="v1") is False
    assert mm2.has_changed(etag="v2") is True


def test_registry_loads():
    from registry.loader import load_registry
    reg = load_registry()
    assert len(reg) > 0
    assert 'st37_wells_txt' in reg.list_all()
    config = reg.get('st37_wells_txt')
    assert config.province == 'AB'


def test_registry_list_by_cadence():
    from registry.loader import load_registry
    reg = load_registry()
    daily = reg.list_by_cadence('daily')
    weekly = reg.list_by_cadence('weekly')
    monthly = reg.list_by_cadence('monthly')
    assert len(daily) > 0
    assert len(weekly) > 0
    assert len(monthly) > 0


def test_registry_get_parser_func():
    from registry.loader import load_registry
    reg = load_registry()
    config = reg.get('st37_wells_txt')
    parser_func = config.get_parser_func()
    assert parser_func is not None
    assert callable(parser_func)


def test_registry_get_parser_func_none():
    from registry.loader import DatasetConfig
    config = DatasetConfig(id="test", name="test", parser="")
    assert config.get_parser_func() is None


def test_dataset_config_link_regex():
    from registry.loader import load_registry
    reg = load_registry()
    config = reg.get('st37_wells_txt')
    assert config.link_regex is not None


def test_validation_required_columns():
    import pandas as pd

    from registry.loader import DatasetConfig, SchemaConfig, ValidationConfig
    from validation.checks import check_required_columns

    df = pd.DataFrame({"uwi": ["a"], "name": ["b"]})
    config = DatasetConfig(
        id="test", name="test",
        schema=SchemaConfig(required_columns=["uwi", "name"]),
        validation=ValidationConfig()
    )
    result = check_required_columns(df, config)
    assert result.passed is True


def test_validation_missing_columns():
    import pandas as pd

    from registry.loader import DatasetConfig, SchemaConfig, ValidationConfig
    from validation.checks import check_required_columns

    df = pd.DataFrame({"uwi": ["a"]})
    config = DatasetConfig(
        id="test", name="test",
        schema=SchemaConfig(required_columns=["uwi", "missing_col"]),
        validation=ValidationConfig()
    )
    result = check_required_columns(df, config)
    assert result.passed is False
    assert "missing_col" in result.message


def test_validation_null_rates():
    import pandas as pd

    from registry.loader import DatasetConfig, SchemaConfig, ValidationConfig
    from validation.checks import check_null_rates

    df = pd.DataFrame({"uwi": [None, None, None, "a"], "name": ["a", "b", "c", "d"]})
    config = DatasetConfig(
        id="test", name="test",
        schema=SchemaConfig(required_columns=["uwi", "name"]),
        validation=ValidationConfig()
    )
    result = check_null_rates(df, config)
    assert result.passed is False
    assert "uwi" in result.message

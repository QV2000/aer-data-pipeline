
import pytest


@pytest.fixture
def tmp_data_dir(tmp_path):
    """Provide a temporary data directory with expected structure."""
    for sub in ['raw', 'bronze', 'silver']:
        (tmp_path / sub).mkdir()
    return tmp_path

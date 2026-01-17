import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from config import Config


def test_config_loads_from_env():
    config = Config()
    assert config.supabase_url.startswith("https://")
    assert config.supabase_key != ""
    assert config.log_level in ["DEBUG", "INFO", "WARNING", "ERROR"]

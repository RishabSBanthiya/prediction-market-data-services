import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

import pytest
from config import Config


def test_config_loads_from_env():
    config = Config()
    # supabase_url may be None if not configured (e.g., using postgres instead)
    if config.supabase_url is not None:
        assert config.supabase_url.startswith("https://")
    if config.supabase_key is not None:
        assert config.supabase_key != ""
    assert config.log_level in ["DEBUG", "INFO", "WARNING", "ERROR"]


def test_config_with_env_vars(monkeypatch):
    monkeypatch.setenv("SUPABASE_URL", "https://test.supabase.co")
    monkeypatch.setenv("SUPABASE_KEY", "test-key")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")

    config = Config()
    assert config.supabase_url == "https://test.supabase.co"
    assert config.supabase_key == "test-key"
    assert config.log_level == "DEBUG"

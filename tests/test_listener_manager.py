import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timezone

from core.listener_factory import ListenerFactory
from core.listener_manager import ListenerManager
from models import ListenerConfig, ListenerFilters


@pytest.fixture
def listener_config():
    return ListenerConfig(
        id="test-listener-1",
        name="test-listener",
        filters=ListenerFilters(tag_ids=[100639]),
        discovery_interval_seconds=60,
        is_active=True,
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )


@pytest.fixture
def mock_supabase():
    return MagicMock()


@pytest.fixture
def mock_logger_factory():
    factory = MagicMock()
    factory.create = MagicMock(return_value=MagicMock())
    return factory


@pytest.fixture
def mock_logger():
    logger = MagicMock()
    logger.info = MagicMock()
    logger.warning = MagicMock()
    return logger


@pytest.fixture
def mock_config_loader(listener_config):
    loader = AsyncMock()
    loader.load_active_configs = AsyncMock(return_value=[listener_config])
    loader.load_config = AsyncMock(return_value=listener_config)
    return loader


def test_listener_factory_creates_listener(mock_supabase, mock_logger_factory, listener_config):
    factory = ListenerFactory(mock_supabase, mock_logger_factory)

    with patch('core.listener_factory.PolymarketDiscoveryService'), \
         patch('core.listener_factory.PolymarketWebSocketClient'), \
         patch('core.listener_factory.SupabaseWriter'):
        listener = factory.create(listener_config)

    assert listener is not None
    assert listener.config == listener_config
    mock_logger_factory.create.assert_called_once_with("listener.test-listener")


@pytest.mark.asyncio
async def test_listener_manager_start(mock_logger, mock_config_loader, mock_logger_factory, mock_supabase):
    factory = ListenerFactory(mock_supabase, mock_logger_factory)
    manager = ListenerManager(factory, mock_config_loader, mock_logger)

    with patch.object(factory, 'create') as mock_create:
        mock_listener = MagicMock()
        mock_listener.start = AsyncMock()
        mock_listener.stop = AsyncMock()
        mock_listener.config = mock_config_loader.load_active_configs.return_value[0]
        mock_listener.listener_id = "test-listener-1"
        mock_create.return_value = mock_listener

        await manager.start()

        mock_config_loader.load_active_configs.assert_called_once()
        mock_create.assert_called_once()
        mock_listener.start.assert_called_once()

        await manager.stop()


@pytest.mark.asyncio
async def test_listener_manager_stop(mock_logger, mock_config_loader, mock_logger_factory, mock_supabase):
    factory = ListenerFactory(mock_supabase, mock_logger_factory)
    manager = ListenerManager(factory, mock_config_loader, mock_logger)

    with patch.object(factory, 'create') as mock_create:
        mock_listener = MagicMock()
        mock_listener.start = AsyncMock()
        mock_listener.stop = AsyncMock()
        mock_listener.config = mock_config_loader.load_active_configs.return_value[0]
        mock_listener.listener_id = "test-listener-1"
        mock_create.return_value = mock_listener

        await manager.start()
        await manager.stop()

        mock_listener.stop.assert_called_once()
        assert len(manager._listeners) == 0


@pytest.mark.asyncio
async def test_listener_manager_get_status(mock_logger, mock_config_loader, mock_logger_factory, mock_supabase):
    factory = ListenerFactory(mock_supabase, mock_logger_factory)
    manager = ListenerManager(factory, mock_config_loader, mock_logger)

    with patch.object(factory, 'create') as mock_create:
        mock_listener = MagicMock()
        mock_listener.start = AsyncMock()
        mock_listener.stop = AsyncMock()
        mock_listener.listener_id = "test-listener-1"
        mock_listener.config.name = "test-listener"
        mock_listener.state.is_running = True
        mock_listener.state.subscribed_markets = {"token-1": MagicMock()}
        mock_listener.state.events_processed = 100
        mock_listener.state.errors_count = 2
        mock_listener.state.last_discovery_at = datetime.now(timezone.utc)
        mock_create.return_value = mock_listener

        await manager.start()
        status = await manager.get_status()

        assert len(status) == 1
        assert status[0]["id"] == "test-listener-1"
        assert status[0]["is_running"] is True
        assert status[0]["subscribed_markets"] == 1
        assert status[0]["events_processed"] == 100
        assert status[0]["errors_count"] == 2

        await manager.stop()


@pytest.mark.asyncio
async def test_listener_manager_reload_adds_new(mock_logger, mock_logger_factory, mock_supabase):
    config1 = ListenerConfig(
        id="listener-1",
        name="listener-1",
        filters=ListenerFilters(),
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )
    config2 = ListenerConfig(
        id="listener-2",
        name="listener-2",
        filters=ListenerFilters(),
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )

    config_loader = AsyncMock()
    config_loader.load_active_configs = AsyncMock(return_value=[config1])

    factory = ListenerFactory(mock_supabase, mock_logger_factory)
    manager = ListenerManager(factory, config_loader, mock_logger)

    with patch.object(factory, 'create') as mock_create:
        mock_listener1 = MagicMock()
        mock_listener1.start = AsyncMock()
        mock_listener1.stop = AsyncMock()
        mock_listener1.config = config1
        mock_listener1.listener_id = "listener-1"

        mock_listener2 = MagicMock()
        mock_listener2.start = AsyncMock()
        mock_listener2.stop = AsyncMock()
        mock_listener2.config = config2
        mock_listener2.listener_id = "listener-2"

        mock_create.side_effect = [mock_listener1, mock_listener2]

        await manager.start()
        assert len(manager._listeners) == 1

        config_loader.load_active_configs.return_value = [config1, config2]
        await manager.reload()
        assert len(manager._listeners) == 2

        await manager.stop()


@pytest.mark.asyncio
async def test_listener_manager_reload_removes_old(mock_logger, mock_logger_factory, mock_supabase):
    config1 = ListenerConfig(
        id="listener-1",
        name="listener-1",
        filters=ListenerFilters(),
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )

    config_loader = AsyncMock()
    config_loader.load_active_configs = AsyncMock(return_value=[config1])

    factory = ListenerFactory(mock_supabase, mock_logger_factory)
    manager = ListenerManager(factory, config_loader, mock_logger)

    with patch.object(factory, 'create') as mock_create:
        mock_listener1 = MagicMock()
        mock_listener1.start = AsyncMock()
        mock_listener1.stop = AsyncMock()
        mock_listener1.config = config1
        mock_listener1.listener_id = "listener-1"
        mock_create.return_value = mock_listener1

        await manager.start()
        assert len(manager._listeners) == 1

        config_loader.load_active_configs.return_value = []
        await manager.reload()
        assert len(manager._listeners) == 0
        mock_listener1.stop.assert_called()

import asyncio
from typing import Optional

from core.listener import Listener
from core.listener_factory import ListenerFactory
from services.config_loader import SupabaseConfigLoader
from models import ListenerConfig


class ListenerManager:
    def __init__(
        self,
        factory: ListenerFactory,
        config_loader: SupabaseConfigLoader,
        logger,
    ):
        self._factory = factory
        self._config_loader = config_loader
        self._logger = logger
        self._listeners: dict[str, Listener] = {}
        self._health_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        self._logger.info("listener_manager_starting")
        configs = await self._config_loader.load_active_configs()
        for config in configs:
            await self._spawn_listener(config)
        self._health_task = asyncio.create_task(self._monitor_health())
        self._logger.info("listener_manager_started", count=len(self._listeners))

    async def stop(self) -> None:
        self._logger.info("listener_manager_stopping")
        if self._health_task:
            self._health_task.cancel()
        await asyncio.gather(*[listener.stop() for listener in self._listeners.values()])
        self._listeners.clear()
        self._logger.info("listener_manager_stopped")

    async def reload(self) -> None:
        self._logger.info("listener_manager_reloading")
        configs = await self._config_loader.load_active_configs()
        config_ids = {c.id for c in configs}
        running_ids = set(self._listeners.keys())

        for listener_id in running_ids - config_ids:
            await self._stop_listener(listener_id)

        for config in configs:
            if config.id not in running_ids:
                await self._spawn_listener(config)

    async def get_status(self) -> list[dict]:
        return [
            {
                "id": listener.listener_id,
                "name": listener.config.name,
                "is_running": listener.state.is_running,
                "subscribed_markets": len(listener.state.subscribed_markets),
                "events_processed": listener.state.events_processed,
                "errors_count": listener.state.errors_count,
                "last_discovery": listener.state.last_discovery_at,
            }
            for listener in self._listeners.values()
        ]

    async def _spawn_listener(self, config: ListenerConfig) -> None:
        listener = self._factory.create(config)
        await listener.start()
        self._listeners[config.id] = listener
        self._logger.info("listener_spawned", name=config.name)

    async def _stop_listener(self, listener_id: str) -> None:
        if listener := self._listeners.pop(listener_id, None):
            await listener.stop()
            self._logger.info("listener_stopped", id=listener_id)

    async def _monitor_health(self) -> None:
        while True:
            await asyncio.sleep(60)
            for listener in self._listeners.values():
                if listener.state.errors_count > 100:
                    self._logger.warning(
                        "listener_high_error_count",
                        name=listener.config.name,
                        errors=listener.state.errors_count,
                    )

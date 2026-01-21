from typing import Optional

from core.interfaces import IDataWriter, IMarketDiscovery, IWebSocketClient
from core.listener import Listener
from services.polymarket_discovery import PolymarketDiscoveryService
from services.polymarket_websocket_client import PolymarketWebSocketClient
from services.supabase_writer import SupabaseWriter
from services.state_forward_filler import StateForwardFiller
from models import ListenerConfig, Platform


class ListenerFactory:
    def __init__(
        self,
        logger_factory,
        supabase_client=None,
        postgres_dsn: Optional[str] = None,
        kalshi_authenticator=None,
    ):
        self._supabase = supabase_client
        self._postgres_dsn = postgres_dsn
        self._logger_factory = logger_factory
        self._kalshi_authenticator = kalshi_authenticator

    def _create_writer(self, listener_id: str, logger, platform: Platform) -> IDataWriter:
        platform_str = platform.value
        if self._postgres_dsn:
            from services.postgres_writer import PostgresWriter
            return PostgresWriter(
                dsn=self._postgres_dsn,
                listener_id=listener_id,
                logger=logger,
                platform=platform_str,
            )
        elif self._supabase:
            return SupabaseWriter(
                client=self._supabase,
                listener_id=listener_id,
                logger=logger,
                platform=platform_str,
            )
        else:
            raise ValueError("Either supabase_client or postgres_dsn must be provided")

    def _create_discovery(self, platform: Platform, logger) -> IMarketDiscovery:
        """Create platform-specific discovery service."""
        if platform == Platform.KALSHI:
            from services.kalshi_discovery import KalshiDiscoveryService
            return KalshiDiscoveryService(logger, self._kalshi_authenticator)
        else:
            return PolymarketDiscoveryService(logger)

    def _create_websocket(self, platform: Platform, logger) -> IWebSocketClient:
        """Create platform-specific WebSocket client."""
        if platform == Platform.KALSHI:
            from services.kalshi_websocket_client import KalshiWebSocketClient
            if not self._kalshi_authenticator:
                raise ValueError(
                    "Kalshi authenticator required for Kalshi platform. "
                    "Set KALSHI_API_KEY and KALSHI_PRIVATE_KEY_PATH environment variables."
                )
            return KalshiWebSocketClient(logger, self._kalshi_authenticator)
        else:
            return PolymarketWebSocketClient(logger)

    def create(self, config: ListenerConfig) -> Listener:
        logger = self._logger_factory.create(f"listener.{config.name}")
        platform = getattr(config, "platform", Platform.POLYMARKET)

        discovery = self._create_discovery(platform, logger)
        websocket = self._create_websocket(platform, logger)
        writer = self._create_writer(config.id, logger, platform)

        forward_filler = None
        if config.enable_forward_fill:
            forward_filler = StateForwardFiller(
                listener_id=config.id,
                logger=logger,
                emit_interval_ms=config.emit_interval_ms,
            )

        return Listener(
            config=config,
            discovery=discovery,
            websocket=websocket,
            writer=writer,
            logger=logger,
            forward_filler=forward_filler,
        )

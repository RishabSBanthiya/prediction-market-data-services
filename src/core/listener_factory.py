from typing import Optional

from core.interfaces import IDataWriter
from core.listener import Listener
from services.market_discovery import PolymarketDiscoveryService
from services.websocket_client import PolymarketWebSocketClient
from services.supabase_writer import SupabaseWriter
from services.state_forward_filler import StateForwardFiller
from models import ListenerConfig


class ListenerFactory:
    def __init__(
        self,
        logger_factory,
        supabase_client=None,
        postgres_dsn: Optional[str] = None,
    ):
        self._supabase = supabase_client
        self._postgres_dsn = postgres_dsn
        self._logger_factory = logger_factory

    def _create_writer(self, listener_id: str, logger) -> IDataWriter:
        if self._postgres_dsn:
            from services.postgres_writer import PostgresWriter
            return PostgresWriter(
                dsn=self._postgres_dsn,
                listener_id=listener_id,
                logger=logger,
            )
        elif self._supabase:
            return SupabaseWriter(
                client=self._supabase,
                listener_id=listener_id,
                logger=logger,
            )
        else:
            raise ValueError("Either supabase_client or postgres_dsn must be provided")

    def create(self, config: ListenerConfig) -> Listener:
        logger = self._logger_factory.create(f"listener.{config.name}")
        discovery = PolymarketDiscoveryService(logger)
        websocket = PolymarketWebSocketClient(logger)
        writer = self._create_writer(config.id, logger)
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

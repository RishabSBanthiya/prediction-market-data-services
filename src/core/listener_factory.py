from core.listener import Listener
from services.market_discovery import PolymarketDiscoveryService
from services.websocket_client import PolymarketWebSocketClient
from services.supabase_writer import SupabaseWriter
from services.state_forward_filler import StateForwardFiller
from models import ListenerConfig


class ListenerFactory:
    def __init__(self, supabase_client, logger_factory):
        self._supabase = supabase_client
        self._logger_factory = logger_factory

    def create(self, config: ListenerConfig) -> Listener:
        logger = self._logger_factory.create(f"listener.{config.name}")
        discovery = PolymarketDiscoveryService(logger)
        websocket = PolymarketWebSocketClient(logger)
        writer = SupabaseWriter(
            client=self._supabase,
            listener_id=config.id,
            logger=logger,
        )
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

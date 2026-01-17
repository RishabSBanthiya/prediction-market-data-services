import asyncio
import signal

from supabase import create_client

from config import Config
from core.listener_factory import ListenerFactory
from core.listener_manager import ListenerManager
from services.config_loader import SupabaseConfigLoader
from utils.logger import LoggerFactory


async def main():
    config = Config()
    logger_factory = LoggerFactory(config.log_level)
    logger = logger_factory.create("main")

    logger.info("application_starting")

    supabase = create_client(config.supabase_url, config.supabase_key)
    config_loader = SupabaseConfigLoader(supabase)
    factory = ListenerFactory(supabase, logger_factory)
    manager = ListenerManager(factory, config_loader, logger)

    loop = asyncio.get_event_loop()
    shutdown_event = asyncio.Event()

    def handle_shutdown(sig):
        logger.info("shutdown_signal_received", signal=sig.name)
        shutdown_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda s=sig: handle_shutdown(s))

    try:
        await manager.start()
        await shutdown_event.wait()
    except Exception as e:
        logger.error("application_error", error=str(e))
    finally:
        await manager.stop()
        logger.info("application_stopped")


if __name__ == "__main__":
    asyncio.run(main())

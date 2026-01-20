import asyncio
import signal

from config import Config
from core.listener_factory import ListenerFactory
from core.listener_manager import ListenerManager
from services.config_loader import SupabaseConfigLoader
from utils.logger import LoggerFactory


async def main():
    config = Config()
    logger_factory = LoggerFactory(config.log_level)
    logger = logger_factory.create("main")

    logger.info("application_starting", db_mode=config.db_mode)

    if config.db_mode == "postgres":
        from services.config_loader import PostgresConfigLoader
        config_loader = PostgresConfigLoader(config.postgres_dsn)
        factory = ListenerFactory(
            logger_factory=logger_factory,
            postgres_dsn=config.postgres_dsn,
        )
    else:
        from supabase import create_client
        supabase = create_client(config.supabase_url, config.supabase_key)
        config_loader = SupabaseConfigLoader(supabase)
        factory = ListenerFactory(
            logger_factory=logger_factory,
            supabase_client=supabase,
        )

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

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

    # Initialize Kalshi authenticator if credentials provided
    kalshi_auth = None
    if config.kalshi_api_key and (config.kalshi_private_key or config.kalshi_private_key_path):
        from services.kalshi_auth import KalshiAuthenticator
        kalshi_auth = KalshiAuthenticator(
            api_key=config.kalshi_api_key,
            private_key_path=config.kalshi_private_key_path,
            private_key_pem=config.kalshi_private_key,
        )
        logger.info("kalshi_auth_initialized")

    if config.db_mode == "postgres":
        from services.config_loader import PostgresConfigLoader
        config_loader = PostgresConfigLoader(config.postgres_dsn)
        factory = ListenerFactory(
            logger_factory=logger_factory,
            postgres_dsn=config.postgres_dsn,
            kalshi_authenticator=kalshi_auth,
        )
    else:
        from supabase import create_client
        supabase = create_client(config.supabase_url, config.supabase_key)
        config_loader = SupabaseConfigLoader(supabase)
        factory = ListenerFactory(
            logger_factory=logger_factory,
            supabase_client=supabase,
            kalshi_authenticator=kalshi_auth,
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

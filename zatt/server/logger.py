import logging
import asyncio
from datetime import datetime
from logging.config import dictConfig
import zatt.server.config as cfg


def tick():
    """Unobtrusive periodic timestamp for debug log."""
    logger = logging.getLogger(__name__)
    logger.debug('Tick: %s', datetime.now().isoformat('T'))
    loop = asyncio.get_event_loop()
    loop.call_later(1, tick)


def start_logger():
    """Configure logging verbosity according to the --debug CLI option."""
    logging_config = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'prod': {'format': '{asctime}: {levelname}: {message}',
                     'style': '{'},
            'develop': {'format': '{message}',
                        'style': '{'}
        },
        'handlers': {
            'console': {'class': 'logging.StreamHandler',
                        'formatter': 'prod',
                        'level': 'DEBUG'}
            },
        'loggers': {
            '': {'handlers': ['console'],
                 'level': 'INFO',
                 'propagate': True,
                 'extra': {'server_id': 1}}
            }
        }
    print (cfg.config.debug)
    if cfg.config.debug:
    	logging_config['handlers']['console']['formatter'] = 'develop'
    	logging_config['loggers']['']['level'] = 'DEBUG'
    	loop = asyncio.get_event_loop()
    	loop.call_later(1, tick)

    dictConfig(logging_config)

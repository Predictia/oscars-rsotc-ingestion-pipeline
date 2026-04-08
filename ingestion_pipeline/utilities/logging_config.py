import logging.config
from typing import Any


def setup_logging(default_level: int = logging.INFO) -> None:
    """
    Configure logging for the application using dictConfig.

    This setup ensures that internal modules have a default level (INFO),
    while specific third-party modules known for being noisy are suppressed
    to a higher level (WARNING).

    Parameters
    ----------
    default_level : int, optional
        The default logging level for the root logger, by default logging.INFO.

    Returns
    -------
    None
    """
    logging_config: dict[str, Any] = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "standard": {
                "format": "%(asctime)s — %(name)s — %(levelname)s — %(message)s",
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "standard",
                "level": default_level,
            },
        },
        "loggers": {
            "": {  # Root logger
                "handlers": ["console"],
                "level": default_level,
                "propagate": True,
            },
            "botocore": {
                "level": "WARNING",
                "propagate": True,
            },
            "aiobotocore": {
                "level": "WARNING",
                "propagate": True,
            },
            "s3fs": {
                "level": "WARNING",
                "propagate": True,
            },
            "urllib3": {
                "level": "WARNING",
                "propagate": True,
            },
            "cdsapi": {
                "level": "WARNING",
                "propagate": True,
            },
            "multiurl": {
                "level": "WARNING",
                "propagate": True,
            },
        },
    }

    logging.config.dictConfig(logging_config)

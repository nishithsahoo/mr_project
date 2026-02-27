from __future__ import annotations

import logging
from pathlib import Path

_LOGGING_CONFIGURED = False


def get_logger(name: str) -> logging.Logger:
    """Get or configure a logger instance for the module.

    Configures logging on first call with both console and file output.
    Subsequent calls return a logger for the specified module name.
    Log file is written to outputs/pipeline.log and overwritten on each run.

    Args:
        name: Name for the logger (typically __name__ from calling module)

    Returns:
        Configured logger instance
    """
    global _LOGGING_CONFIGURED

    # Configure logging once on first call
    if not _LOGGING_CONFIGURED:
        log_path = Path("outputs") / "pipeline.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)

        handler = logging.StreamHandler()
        file_handler = logging.FileHandler(log_path, mode="w", encoding="utf-8")
        formatter = logging.Formatter(
            "%(asctime)s %(levelname)s %(name)s - %(message)s"
        )
        handler.setFormatter(formatter)
        file_handler.setFormatter(formatter)

        root_logger = logging.getLogger()
        root_logger.handlers.clear()
        root_logger.addHandler(handler)
        root_logger.addHandler(file_handler)
        root_logger.setLevel(logging.INFO)

        _LOGGING_CONFIGURED = True

    logger = logging.getLogger(name)
    logger.propagate = True
    return logger

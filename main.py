"""Main pipeline execution script.

Runs data processing pipelines in sequence: call, edetail, events, vae, and hco.
Cleans output directory before execution and logs all operations to outputs/pipeline.log.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

from pipelines import call, edetail, events, hco_promotion, vae
from utils.logging import get_logger

# Pipeline configuration file paths
CONFIGS = {
    "call": "config/call.json",
    "edetail": "config/edetail.json",
    "events": "config/events.json",
    "vae": "config/vae.json",
    "hco": "config/hco_promotion.json",
}


def load_config(path: str) -> Dict[str, Any]:
    """Load JSON configuration file.

    Args:
        path: Path to the JSON configuration file

    Returns:
        Dictionary containing configuration settings
    """
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def run_pipeline(name: str, config_path: str) -> None:
    """Execute a single data processing pipeline.

    Loads configuration, runs the specified pipeline, and logs execution.
    If an error occurs, logs the error message and re-raises the exception.

    Args:
        name: Name of the pipeline to run ('call', 'edetail', 'events', 'vae', 'hco')
        config_path: Path to the pipeline's JSON configuration file

    Raises:
        ValueError: If pipeline name is unknown
        Exception: Any exception raised during pipeline execution
    """
    logger = get_logger(__name__)

    try:
        # Load pipeline configuration
        config = load_config(config_path)
        logger.info("Running %s pipeline with config %s", name, config_path)

        # Execute the specified pipeline
        if name == "call":
            call.run(config)
        elif name == "edetail":
            edetail.run(config)
        elif name == "events":
            events.run(config)
        elif name == "vae":
            vae.run(config)
        elif name == "hco":
            hco_promotion.run(config)
        else:
            raise ValueError(f"Unknown pipeline: {name}")

        logger.info("Completed %s pipeline", name)
    except Exception as e:
        # Log error without full traceback and re-raise
        logger.error("Failed to run %s pipeline: %s: %s", name, type(e).__name__, e)
        raise


def main() -> None:
    """Main entry point for pipeline execution.

    Cleans output directory (except pipeline.log), then runs all pipelines
    in sequence: call, edetail, events, vae, and hco.
    """
    # Clean outputs folder before running pipelines (except pipeline.log)
    outputs_dir = Path("outputs")
    if outputs_dir.exists():
        for file in outputs_dir.iterdir():
            if file.is_file() and file.name != "pipeline.log":
                file.unlink()

    logger = get_logger(__name__)
    logger.info("Starting pipeline execution")

    # Run all pipelines in sequence
    for pipeline_name in ["call", "edetail", "events", "vae", "hco"]:
        config_path = CONFIGS[pipeline_name]
        run_pipeline(pipeline_name, config_path)


if __name__ == "__main__":
    main()

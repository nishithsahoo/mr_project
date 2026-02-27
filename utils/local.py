from pathlib import Path


def ensure_parent_dir(path: str) -> None:
    """Create parent directories for a file path if they don't exist.

    Args:
        path: File path for which to create parent directories
    """
    Path(path).parent.mkdir(parents=True, exist_ok=True)

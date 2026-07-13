"""Standalone import adapter.

The full workspace mutates ``sys.path`` here. The standalone systemd unit sets
``PYTHONPATH`` explicitly, so the compatible entry point is intentionally a no-op.
"""


def enable(_current_file: str) -> None:
    """Keep the workspace API while relying on the unit's explicit PYTHONPATH."""


setup_imports = enable

"""
Utility functions for ParseEngine.
"""

import logging

def configure_logging(enable_logging: bool = True, level: int = logging.DEBUG):
    """
    Configure logging settings.

    Args:
        enable_logging (bool): Whether to enable logging
        level (int): Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    if enable_logging:
        logging.basicConfig(
            level=level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    else:
        logging.basicConfig(level=logging.CRITICAL)  # Disable all logging except critical errors 
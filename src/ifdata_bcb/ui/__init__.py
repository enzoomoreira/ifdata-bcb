"""
Modulo de interface de usuario para ifdata-bcb.

Este modulo contem:
- display: Display singleton thread-safe com Rich
"""

from ifdata_bcb.ui.display import Display, get_display

__all__ = [
    "Display",
    "get_display",
]

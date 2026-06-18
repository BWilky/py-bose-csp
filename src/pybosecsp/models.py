"""Data models for the bose-csp-api library."""

from dataclasses import dataclass


@dataclass
class ZoneState:
    """Represents the current state of a single audio zone."""

    volume: float = 0.0
    is_muted: bool = False
    current_source: int = 0

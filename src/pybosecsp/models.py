"""Data models for the py-bose-csp library."""

from dataclasses import dataclass


@dataclass
class ZoneState:
    """Represents the current state of a single audio zone."""

    volume: float = 0.0
    is_muted: bool = False
    current_source: int = 0
    # True when the zone's AutoVolume is active. While On, the device rejects
    # manual gain sets (it rides the level from ambient noise), so callers
    # should not attempt to set volume on this zone.
    auto_volume: bool = False

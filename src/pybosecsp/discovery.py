"""WebSocket discovery for Bose CSP devices."""

import asyncio
import json
import logging
from typing import Any
import websockets

from .exceptions import BoseCSPConnectionError

_LOGGER = logging.getLogger(__name__)


async def discover_zones_and_sources(
    host: str, timeout: float = 10.0
) -> dict[str, list[dict[str, Any]]]:
    """Discover zones and sources on a Bose CSP device via WebSockets.

    Args:
        host: IP address of the Bose CSP device.
        timeout: Timeout in seconds for the connection and discovery.

    Returns:
        A dict containing:
        - "zones": List of dicts with {"id": int, "label": str, "enabled": bool,
          "min_gain": float, "max_gain": float, "gain": float}
        - "sources": List of dicts with {"id": int, "label": str, "enabled": bool}

    Raises:
        BoseCSPConnectionError: If the WebSocket connection fails or is closed.
    """
    uri = f"ws://{host}/cmd"
    _LOGGER.info("Attempting WebSocket connection to %s for auto-discovery", uri)

    try:
        async with websockets.connect(uri, open_timeout=timeout) as websocket:
            # 1. Request Sources (Inputs)
            source_request = {
                "id": 1001,
                "event": "retrieve",
                "type": "source",
            }
            await websocket.send(json.dumps(source_request))

            # 2. Request Zones (Listening Areas)
            zone_request = {
                "id": 1002,
                "event": "retrieve",
                "type": "zone",
            }
            await websocket.send(json.dumps(zone_request))

            sources: list[dict[str, Any]] | None = None
            zones: list[dict[str, Any]] | None = None

            # 3. Listen for the responses
            async def read_responses() -> None:
                nonlocal sources, zones
                while sources is None or zones is None:
                    message_str = await websocket.recv()
                    if not message_str.strip():
                        continue

                    message = json.loads(message_str)
                    if message.get("event") == "retrieved":
                        msg_type = message.get("type")
                        data = message.get("data", [])

                        if msg_type == "source":
                            sources = [
                                {
                                    "id": src.get("sourceId"),
                                    "label": src.get("label"),
                                    "enabled": src.get("enabled", False),
                                }
                                for src in data
                            ]
                        elif msg_type == "zone":
                            zones = [
                                {
                                    "id": zone.get("zoneId"),
                                    "label": zone.get("label"),
                                    "enabled": zone.get("enabled", False),
                                    "min_gain": zone.get("minGain"),
                                    "max_gain": zone.get("maxGain"),
                                    "gain": zone.get("gain"),
                                }
                                for zone in data
                            ]

            await asyncio.wait_for(read_responses(), timeout=timeout)

            return {
                "zones": zones or [],
                "sources": sources or [],
            }

    except websockets.exceptions.ConnectionClosed as e:
        _LOGGER.error(
            "WebSocket connection closed by device during discovery: %s", e
        )
        raise BoseCSPConnectionError(
            "WebSocket connection closed by the device. "
            "Note: The Bose CSP only allows ONE active configuration session at a time. "
            "Please ensure the Web Dashboard is closed in your browser."
        ) from e
    except (OSError, asyncio.TimeoutError) as e:
        _LOGGER.error(
            "Failed to connect or communicate with device at %s: %s", host, e
        )
        raise BoseCSPConnectionError(
            f"Could not reach the device at {host}. Is it turned on and connected to the network?"
        ) from e
    except Exception as e:
        _LOGGER.error("Unexpected error during Bose CSP discovery: %s", e)
        raise BoseCSPConnectionError(
            f"Unexpected error during discovery: {e}"
        ) from e

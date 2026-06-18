# py-bose-csp

Async Python library for controlling Bose CSP (Commercial Sound Processor) audio devices over TCP.

## Installation

```bash
pip install py-bose-csp
```

## Quick Start

```python
import asyncio
from pybosecsp import BoseCSPDevice, ZoneState

async def main():
    device = BoseCSPDevice("192.168.1.100", ["Zone1", "Zone2"])

    await device.connect()

    # Get current state
    state: ZoneState = device.get_zone_state("Zone1")
    print(f"Volume: {state.volume}, Muted: {state.is_muted}, Source: {state.current_source}")

    # Control the device
    await device.set_volume("Zone1", -20.0)
    await device.set_mute("Zone1", True)
    await device.set_source("Zone1", 2)

    # Subscribe to state changes
    def on_update(zone_name: str) -> None:
        new_state = device.get_zone_state(zone_name)
        print(f"{zone_name} updated: {new_state}")

    device.subscribe_updates(on_update)

    # Subscribe to availability changes
    def on_availability(is_available: bool) -> None:
        print(f"Device available: {is_available}")

    device.subscribe_availability(on_availability)

    # ... keep running ...

    await device.disconnect()

asyncio.run(main())
```

## API Reference

### `BoseCSPDevice(host, zones, port=10055)`

Main device class. Creates a TCP connection to a Bose CSP device.

**Parameters:**
- `host` — IP address or hostname of the device
- `zones` — List of zone (listening area) names configured on the device
- `port` — TCP port (default: `10055`)
- `reconnect_delay` — Seconds between reconnection attempts (default: `5`)
- `volume_interval` — Seconds between volume polls (default: `5`)
- `other_interval` — Seconds between mute/source polls (default: `30`)

**Methods:**
- `await device.connect()` — Connect and start polling
- `await device.disconnect()` — Disconnect and clean up
- `device.get_all_states() -> dict[str, ZoneState]` — Get a copy of all zone states
- `device.get_zone_state(zone_name) -> ZoneState` — Get a copy of a single zone's state
- `await device.set_volume(zone_name, volume_db)` — Set zone volume in dB
- `await device.set_mute(zone_name, mute_on)` — Set zone mute state
- `await device.set_source(zone_name, source_index)` — Set zone input source
- `device.subscribe_updates(callback)` — Register for zone state change callbacks
- `device.unsubscribe_updates(callback)` — Unregister a state change callback
- `device.subscribe_availability(callback)` — Register for connection status callbacks
- `device.unsubscribe_availability(callback)` — Unregister a connection status callback

**Properties:**
- `device.host: str` — Device hostname/IP
- `device.is_connected: bool` — Current connection status

### `ZoneState`

Dataclass representing a zone's state:
- `volume: float` — Current volume in dB (default: `0.0`)
- `is_muted: bool` — Mute state (default: `False`)
- `current_source: int` — Active source index (default: `0`)

### Exceptions

- `BoseCSPError` — Base exception
- `BoseCSPConnectionError` — Connection failures
- `BoseCSPCommandError` — Command failures

## Protocol

This library implements the Bose CSP serial control protocol over TCP (default port 10055).
See the [Bose CSP Serial Control Guide](https://assets.boseprofessional.com/m/48b4f11e8a4922b9/original/ug_csp_control_serial.pdf) for full protocol documentation.

## License

MIT

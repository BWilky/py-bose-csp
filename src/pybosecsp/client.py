"""
Async client for controlling Bose CSP audio devices over TCP.

This module provides the BoseCSPDevice class which manages a persistent TCP
connection to a Bose Commercial Sound Processor, handles automatic reconnection,
periodic state polling, optimistic updates, and push-based state change
notifications.
"""

import asyncio
import copy
import logging
import re
from collections.abc import Callable

from .exceptions import BoseCSPConnectionError
from .models import ZoneState

_LOGGER = logging.getLogger(__name__)


class BoseCSPDevice:
    """Asyncio client for Bose CSP devices.

    Manages a TCP connection to a Bose CSP device, providing methods
    to control volume, mute, and source selection for configured zones.
    Supports automatic reconnection, periodic polling, and optimistic
    state updates with debounce logic.
    """

    def __init__(
        self,
        host: str,
        zones: list[str],
        port: int = 10055,
        reconnect_delay: int = 5,
        volume_interval: int = 5,
        other_interval: int = 30,
    ) -> None:
        """Initialize the BoseCSPDevice.

        Args:
            host: IP address or hostname of the Bose CSP device.
            zones: List of zone (listening area) names configured on the device.
            port: TCP port for the CSP control protocol.
            reconnect_delay: Seconds to wait between reconnection attempts.
            volume_interval: Seconds between volume polling cycles.
            other_interval: Seconds between mute/source polling cycles.
        """
        self.host: str = host
        self.port: int = port
        self._zones: list[str] = list(zones)
        self._reconnect_delay: int = reconnect_delay
        self._volume_interval: int = volume_interval
        self._other_interval: int = other_interval

        # Internal state storage using ZoneState dataclasses
        self._state: dict[str, ZoneState] = {
            zone: ZoneState() for zone in self._zones
        }

        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._lock: asyncio.Lock = asyncio.Lock()

        # Publicly readable connection status
        self.is_connected: bool = False

        self._running: bool = False
        self._listener_task: asyncio.Task | None = None
        self._query_task: asyncio.Task | None = None
        self._reconnect_task: asyncio.Task | None = None

        # Data update callbacks (per-zone)
        self._update_callbacks: list[Callable[[str], None]] = []

        # Availability callbacks (global)
        self._availability_callbacks: list[Callable[[bool], None]] = []

        # Dictionaries to hold ignore flags for optimistic updates
        self._ignore_volume_update: dict[str, bool] = {}
        self._ignore_mute_update: dict[str, bool] = {}
        self._ignore_source_update: dict[str, bool] = {}

        # Pre-compile regex patterns for response parsing
        self._gain_re: re.Pattern[str] = re.compile(r'GA"(.+?) Gain">1=(.*)')
        self._mute_re: re.Pattern[str] = re.compile(r'GA"(.+?) Gain">2=(.*)')
        self._source_re: re.Pattern[str] = re.compile(
            r'GA"(.+?) Selector">1=(.*)'
        )

    # ------------------------------------------------------------------ #
    #  Public state access
    # ------------------------------------------------------------------ #

    def get_all_states(self) -> dict[str, ZoneState]:
        """Return a copy of the current state for all zones.

        Returns:
            A dictionary mapping zone names to copies of their ZoneState.
        """
        return {zone: copy.copy(state) for zone, state in self._state.items()}

    def get_zone_state(self, zone_name: str) -> ZoneState:
        """Return a copy of the current state for a single zone.

        Args:
            zone_name: The name of the zone to query.

        Returns:
            A copy of the ZoneState for the requested zone.

        Raises:
            KeyError: If the zone name is not known.
        """
        return copy.copy(self._state[zone_name])

    # ------------------------------------------------------------------ #
    #  Subscription management
    # ------------------------------------------------------------------ #

    def subscribe_updates(self, callback: Callable[[str], None]) -> None:
        """Register a callback for zone state change notifications.

        The callback receives the zone name that changed as its argument.
        """
        self._update_callbacks.append(callback)

    def unsubscribe_updates(self, callback: Callable[[str], None]) -> None:
        """Unregister a previously registered state change callback."""
        if callback in self._update_callbacks:
            self._update_callbacks.remove(callback)

    def subscribe_availability(self, callback: Callable[[bool], None]) -> None:
        """Register a callback for connection availability notifications.

        The callback receives a boolean indicating whether the device is
        currently reachable.
        """
        self._availability_callbacks.append(callback)

    def unsubscribe_availability(
        self, callback: Callable[[bool], None]
    ) -> None:
        """Unregister a previously registered availability callback."""
        if callback in self._availability_callbacks:
            self._availability_callbacks.remove(callback)

    # ------------------------------------------------------------------ #
    #  Connection lifecycle
    # ------------------------------------------------------------------ #

    def _notify_availability(self, is_available: bool) -> None:
        """Notify all registered listeners of an availability change."""
        if self.is_connected == is_available:
            return  # No change

        _LOGGER.info("Setting connection status to: %s", is_available)
        self.is_connected = is_available
        for callback in self._availability_callbacks:
            callback(is_available)

    async def connect(self) -> None:
        """Connect to the device and start listener/query tasks.

        Raises:
            BoseCSPConnectionError: If the connection cannot be established.
        """
        if self.is_connected:
            _LOGGER.warning("Already connected.")
            return

        # Clean up any stale tasks/socket from a previous connection
        # before creating new ones (prevents zombie task accumulation
        # across reconnects).
        await self._cleanup_connection()

        _LOGGER.info("Attempting to connect to %s:%s...", self.host, self.port)
        try:
            connect_coro = asyncio.open_connection(self.host, self.port)
            self._reader, self._writer = await asyncio.wait_for(
                connect_coro, timeout=5
            )
            self._running = True

            self._listener_task = asyncio.create_task(self._listen())
            self._query_task = asyncio.create_task(self._periodic_query())

            _LOGGER.info(
                "Successfully connected to %s:%s.", self.host, self.port
            )
            self._notify_availability(True)
            await self.query_all_zones_state()

        except (OSError, asyncio.TimeoutError) as err:
            _LOGGER.error("Connection failed: %s", err)
            self._notify_availability(False)
            raise BoseCSPConnectionError(
                "Failed to connect to %s:%s" % (self.host, self.port)
            ) from err

    async def disconnect(self) -> None:
        """Disconnect from the device and clean up all tasks."""
        _LOGGER.info("Disconnecting...")
        self._running = False
        self._notify_availability(False)

        # Cancel reconnect task first to prevent it from re-triggering
        if self._reconnect_task:
            self._reconnect_task.cancel()
            self._reconnect_task = None

        await self._cleanup_connection()
        _LOGGER.info("Disconnected.")

    async def _cleanup_connection(self) -> None:
        """Cancel listener/query tasks and close the socket.

        Safe to call multiple times. Used by both disconnect() and
        connect() (to clean up stale state before reconnecting).
        """
        for task in [self._listener_task, self._query_task]:
            if task and not task.done():
                task.cancel()
        self._listener_task = None
        self._query_task = None

        if self._writer:
            self._writer.close()
            try:
                await self._writer.wait_closed()
            except Exception:  # noqa: BLE001
                pass

        self._writer = None
        self._reader = None

    # ------------------------------------------------------------------ #
    #  Reconnection
    # ------------------------------------------------------------------ #

    def _start_reconnect(self) -> None:
        """Schedule a reconnection attempt."""
        # Only notify if we weren't already marked as disconnected
        if self.is_connected:
            self._notify_availability(False)

        if self._running and (
            not self._reconnect_task or self._reconnect_task.done()
        ):
            _LOGGER.info("Scheduling reconnection...")
            self._reconnect_task = asyncio.create_task(
                self._handle_reconnect()
            )

    async def _handle_reconnect(self) -> None:
        """Wait for the configured delay and then attempt to reconnect."""
        if not self._running:
            return

        _LOGGER.info(
            "Waiting %ss before reconnecting...", self._reconnect_delay
        )
        await asyncio.sleep(self._reconnect_delay)

        if self._running:
            try:
                await self.connect()
            except asyncio.CancelledError:
                return  # Shutdown in progress, don't retry
            except Exception:  # noqa: BLE001
                # Catch everything so the reconnect loop never dies
                self._start_reconnect()

    # ------------------------------------------------------------------ #
    #  TCP listener
    # ------------------------------------------------------------------ #

    async def _listen(self) -> None:
        """Listen for incoming data and parse responses."""
        buffer = ""
        while self._running and self._reader:
            try:
                data = await self._reader.read(1024)
                if not data:
                    _LOGGER.warning("Connection closed by remote end.")
                    self._start_reconnect()
                    return

                buffer += data.decode("utf-8")
                while "\r" in buffer:
                    line, buffer = buffer.split("\r", 1)
                    self._parse_response(line)

            except (OSError, ConnectionResetError) as err:
                _LOGGER.error("Connection error in listener: %s", err)
                self._start_reconnect()
                return
            except asyncio.CancelledError:
                return
            except Exception as err:  # noqa: BLE001
                _LOGGER.error("Unexpected error in listener: %s", err)

    # ------------------------------------------------------------------ #
    #  Periodic polling
    # ------------------------------------------------------------------ #

    async def _periodic_query(self) -> None:
        """Periodically poll the device state.

        Volume is polled every ``volume_interval`` seconds.  Mute and source
        are polled every ``other_interval`` seconds using a tick counter.
        """
        # Calculate ticks, ensuring at least one tick
        ticks_per_other_query = max(
            1, self._other_interval // self._volume_interval
        )
        tick_count = 0

        while self._running:
            try:
                if not self.is_connected:
                    await asyncio.sleep(self._volume_interval)
                    continue

                for zone in self._zones:
                    # Only query if we are not ignoring updates for this zone
                    if not self._ignore_volume_update.get(zone):
                        await self.query_volume(zone)
                    await asyncio.sleep(0.1)

                if tick_count % ticks_per_other_query == 0:
                    for zone in self._zones:
                        if not self._ignore_mute_update.get(zone):
                            await self.query_mute(zone)
                        await asyncio.sleep(0.1)
                        if not self._ignore_source_update.get(zone):
                            await self.query_source(zone)
                        await asyncio.sleep(0.1)

                tick_count += 1
                await asyncio.sleep(self._volume_interval)

            except asyncio.CancelledError:
                return
            except Exception as err:  # noqa: BLE001
                _LOGGER.error("Error in periodic query: %s", err)

    # ------------------------------------------------------------------ #
    #  Response parsing
    # ------------------------------------------------------------------ #

    def _parse_response(self, response: str) -> None:
        """Parse a response string and update internal state."""
        response = response.strip()
        if not response or response == "<ACK>":
            return
        if response.startswith("<NAK>"):
            _LOGGER.warning("Command failed: %s", response)
            return

        _LOGGER.debug("Received: %s", response)
        updated_zone: str | None = None
        try:
            if m := self._gain_re.match(response):
                area, value = m.groups()
                # Check ignore flag
                if self._ignore_volume_update.get(area):
                    _LOGGER.debug(
                        "Ignoring volume update for %s due to debounce", area
                    )
                    return
                if area in self._state:
                    new_vol = float(value)
                    if self._state[area].volume != new_vol:
                        self._state[area].volume = new_vol
                        updated_zone = area
            elif m := self._mute_re.match(response):
                area, value = m.groups()
                # Check ignore flag
                if self._ignore_mute_update.get(area):
                    _LOGGER.debug(
                        "Ignoring mute update for %s due to debounce", area
                    )
                    return
                if area in self._state:
                    new_mute = value == "O"
                    if self._state[area].is_muted != new_mute:
                        self._state[area].is_muted = new_mute
                        updated_zone = area
            elif m := self._source_re.match(response):
                area, value = m.groups()
                # Check ignore flag
                if self._ignore_source_update.get(area):
                    _LOGGER.debug(
                        "Ignoring source update for %s due to debounce", area
                    )
                    return
                if area in self._state:
                    new_source = int(value)
                    if self._state[area].current_source != new_source:
                        self._state[area].current_source = new_source
                        updated_zone = area

        except Exception as err:  # noqa: BLE001
            _LOGGER.error("Error parsing response '%s': %s", response, err)

        if updated_zone:
            # If we successfully parsed data, we must be connected
            self._notify_availability(True)

            _LOGGER.info(
                "State change for '%s': %s",
                updated_zone,
                self._state[updated_zone],
            )
            for callback in self._update_callbacks:
                callback(updated_zone)

    # ------------------------------------------------------------------ #
    #  Command sending
    # ------------------------------------------------------------------ #

    async def _send_command(self, command: str) -> None:
        """Send a raw command string to the device over the TCP socket."""
        if not self.is_connected or not self._writer:
            _LOGGER.warning("Not connected. Command not sent: %s", command)
            return

        _LOGGER.debug("Sending: %s", command)
        try:
            async with self._lock:
                self._writer.write(("%s\r" % command).encode("utf-8"))
                await self._writer.drain()
        except OSError as err:
            _LOGGER.error("Error sending command '%s': %s", command, err)
            self._start_reconnect()

    # ------------------------------------------------------------------ #
    #  Optimistic update helpers
    # ------------------------------------------------------------------ #

    def _fire_update_callback(self, zone_name: str) -> None:
        """Manually fire the update callback for a zone."""
        _LOGGER.debug("Firing optimistic update for %s", zone_name)
        for callback in self._update_callbacks:
            callback(zone_name)

    async def _set_ignore_flag(
        self, flag_dict: dict[str, bool], key: str, delay: float
    ) -> None:
        """Set a flag to temporarily ignore polled state updates.

        This prevents polled responses from overwriting an optimistic
        update before the device has had time to process the command.
        """
        flag_dict[key] = True
        await asyncio.sleep(delay)
        flag_dict[key] = False
        _LOGGER.debug("Cleared ignore flag for %s", key)

    # ------------------------------------------------------------------ #
    #  Public control methods
    # ------------------------------------------------------------------ #

    async def set_volume(self, zone_name: str, volume_db: float) -> None:
        """Set the gain level for a zone and optimistically update state.

        Args:
            zone_name: The zone to control.
            volume_db: The desired volume in dB.
        """
        # Only send if state is different
        if self._state[zone_name].volume == volume_db:
            return

        # Set ignore flag
        asyncio.create_task(
            self._set_ignore_flag(self._ignore_volume_update, zone_name, 2.0)
        )

        # Optimistic update (before await to be synchronous)
        self._state[zone_name].volume = volume_db
        self._fire_update_callback(zone_name)

        cmd = 'SA"%s Gain">1=%.1f' % (zone_name, volume_db)
        await self._send_command(cmd)

    async def set_mute(self, zone_name: str, mute_on: bool) -> None:
        """Set the mute state for a zone and optimistically update state.

        Args:
            zone_name: The zone to control.
            mute_on: True to mute, False to unmute.
        """
        # Only send if state is different
        if self._state[zone_name].is_muted == mute_on:
            return

        # Set ignore flag
        asyncio.create_task(
            self._set_ignore_flag(self._ignore_mute_update, zone_name, 2.0)
        )

        # Optimistic update (before await to be synchronous)
        self._state[zone_name].is_muted = mute_on
        self._fire_update_callback(zone_name)

        state_char = "O" if mute_on else "F"
        cmd = 'SA"%s Gain">2=%s' % (zone_name, state_char)
        await self._send_command(cmd)

    async def set_source(self, zone_name: str, source_index: int) -> None:
        """Set the input source for a zone and optimistically update state.

        Args:
            zone_name: The zone to control.
            source_index: The source index to select.
        """
        # Only send if state is different
        if self._state[zone_name].current_source == source_index:
            return

        # Set ignore flag
        asyncio.create_task(
            self._set_ignore_flag(self._ignore_source_update, zone_name, 2.0)
        )

        # Optimistic update (before await to be synchronous)
        self._state[zone_name].current_source = source_index
        self._fire_update_callback(zone_name)

        cmd = 'SA"%s Selector">1=%s' % (zone_name, source_index)
        await self._send_command(cmd)

    # ------------------------------------------------------------------ #
    #  Public query methods
    # ------------------------------------------------------------------ #

    async def query_volume(self, zone_name: str) -> None:
        """Query the current volume for a zone."""
        cmd = 'GA"%s Gain">1' % zone_name
        await self._send_command(cmd)

    async def query_mute(self, zone_name: str) -> None:
        """Query the current mute state for a zone."""
        cmd = 'GA"%s Gain">2' % zone_name
        await self._send_command(cmd)

    async def query_source(self, zone_name: str) -> None:
        """Query the current source selection for a zone."""
        cmd = 'GA"%s Selector">1' % zone_name
        await self._send_command(cmd)

    async def query_all_zones_state(self) -> None:
        """Query volume, mute, and source for all configured zones."""
        _LOGGER.info("Querying all device states...")
        for zone in self._zones:
            await self.query_volume(zone)
            await asyncio.sleep(0.1)
            await self.query_mute(zone)
            await asyncio.sleep(0.1)
            await self.query_source(zone)
            await asyncio.sleep(0.1)

"""
Async client for controlling Bose CSP audio devices over TCP.

This module provides the BoseCSPDevice class which manages a persistent TCP
connection to a Bose Commercial Sound Processor, handles automatic reconnection,
periodic state polling, and optimistic updates.

The Bose CSP Serial Control Protocol (SoIP, port 10055) is request/response
only: the device sends no unsolicited events. State changes made elsewhere
(e.g. a physical CC zone controller) are picked up by polling with ``GA``
queries; the subscriber callbacks fire as those poll responses are parsed
asynchronously, not from any device-initiated push.
"""

import asyncio
import copy
import errno
import logging
import re
import socket
from collections.abc import Callable

from .exceptions import BoseCSPConnectionError
from .models import ZoneState

_LOGGER = logging.getLogger(__name__)

# --- Active "Health Checking" probe tuning -------------------------------- #
# The minimum gain step the CSP accepts (0.5 dB per the Serial Control Protocol
# Guide); the probe nudges by exactly one step so the change is inaudible.
MICRO_STEP_DB = 0.5
# Seconds between health-check cycles.
HEALTH_INTERVAL = 1800
# Seconds to wait after the nudge before reading the value back.
HEALTH_WAIT = 10
# Seconds to wait between failed attempts within a cycle.
HEALTH_RETRY_DELAY = 30
# Attempts (initial + retries) before a cycle gives up and forces a reconnect.
HEALTH_MAX_ATTEMPTS = 3

# Health status values reported via the health callback.
HEALTH_DISABLED = "disabled"
HEALTH_STARTING = "starting"
HEALTH_CHECKING = "checking"
HEALTH_HEALTHY = "healthy"
HEALTH_NO_ZONE = "not available - auto volume"
HEALTH_SOCKET_NOT_CONNECTED = "Socket Not Connected"
HEALTH_FAILING = "failing"
HEALTH_CANT_RECONNECT = "cant_reconnect"


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
        health_check_enabled: bool = True,
        zone_limits: dict[str, tuple[float, float]] | None = None,
    ) -> None:
        """Initialize the BoseCSPDevice.

        Args:
            host: IP address or hostname of the Bose CSP device.
            zones: List of zone (listening area) names configured on the device.
            port: TCP port for the CSP control protocol.
            reconnect_delay: Seconds to wait between reconnection attempts.
            volume_interval: Seconds between volume polling cycles.
            other_interval: Seconds between mute/source polling cycles.
            health_check_enabled: Whether to run the active control-verify
                "Health Checking" probe.
            zone_limits: Optional {zone: (min_db, max_db)} map used to keep the
                health-check nudge within each zone's floor/ceiling.
        """
        self.host: str = host
        self.port: int = port
        self._zones: list[str] = list(zones)
        self._zone_limits: dict[str, tuple[float, float]] = dict(
            zone_limits or {}
        )
        self._reconnect_delay: int = reconnect_delay
        # Cap on the exponential reconnect backoff. Kept modest so that once the
        # device frees the SoIP port (e.g. the web dashboard is closed) the
        # integration recovers within one backoff window rather than minutes.
        self._reconnect_backoff_max: int = 30
        self._volume_interval: int = volume_interval
        self._other_interval: int = other_interval

        # Internal state storage using ZoneState dataclasses
        self._state: dict[str, ZoneState] = {
            zone: ZoneState() for zone in self._zones
        }

        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._lock: asyncio.Lock = asyncio.Lock()
        # Serializes connect() so two callers (e.g. a lingering internal
        # reconnect racing a reload/setup-retry) can never open two overlapping
        # TCP sockets at once. SoIP/10055 itself supports concurrent control
        # sessions (per the Bose CSP Serial Control Protocol Guide), but two
        # sockets from *this* client would fight over the same reader/writer.
        self._connect_lock: asyncio.Lock = asyncio.Lock()

        # Publicly readable connection status
        self.is_connected: bool = False

        # Set when the device sends its first byte after a (re)connect. SoIP
        # control (port 10055) coexists with the ControlSpace Remote app and
        # CC-1D/2D/3D zone controllers, but the device's browser configuration
        # dashboard holds an *exclusive* session: while it is open the CSP may
        # accept the TCP socket and then never answer (or refuse it outright).
        # connect() waits on this so a dead/held session surfaces as a failure
        # rather than a device stuck at default 0 dB / no source.
        self._alive_event: asyncio.Event = asyncio.Event()
        # Max seconds connect() waits for that first response before failing.
        self._connect_verify_timeout: float = 10.0

        self._running: bool = False
        self._listener_task: asyncio.Task | None = None
        self._query_task: asyncio.Task | None = None
        self._reconnect_task: asyncio.Task | None = None

        # Strong references to fire-and-forget tasks (e.g. ignore-flag
        # timers) so they are not garbage-collected before completion.
        self._background_tasks: set[asyncio.Task] = set()

        # Consecutive failed reconnect attempts, used for exponential
        # backoff so a device that keeps dropping/refusing us (e.g. its web
        # configuration dashboard is open and holds an exclusive session) is
        # not hammered into a lockup.
        self._reconnect_attempts: int = 0

        # Data update callbacks (per-zone)
        self._update_callbacks: list[Callable[[str], None]] = []

        # Availability callbacks (global)
        self._availability_callbacks: list[Callable[[bool], None]] = []

        # Health-status callbacks (global). Carry one of the HEALTH_* strings.
        self._health_callbacks: list[Callable[[str], None]] = []
        self._health_status: str = HEALTH_DISABLED

        # Dictionaries to hold ignore flags for optimistic updates
        self._ignore_volume_update: dict[str, bool] = {}
        self._ignore_mute_update: dict[str, bool] = {}
        self._ignore_source_update: dict[str, bool] = {}
        self._ignore_av_update: dict[str, bool] = {}

        # --- Active "Health Checking" probe state --------------------------- #
        self._health_check_enabled: bool = health_check_enabled
        self._health_task: asyncio.Task | None = None
        # Chosen probe zone, in-memory only (never persisted): re-derived on
        # every (re)connect so it stays in sync with current AutoVolume state.
        self._health_zone: str | None = None
        # When a probe is in flight, gain readbacks for this zone are routed to
        # _probe_value/_probe_event instead of public state (see
        # _parse_response), so the probe never fires a state-change callback.
        self._probe_zone: str | None = None
        self._probe_value: float | None = None
        self._probe_event: asyncio.Event = asyncio.Event()
        # Set if a user-originated set_volume() hits the probe zone mid-window,
        # so a deliberate change is classified inconclusive rather than failed.
        self._health_user_change: bool = False

        # Pre-compile regex patterns for response parsing.
        # The device echoes responses with optional whitespace, e.g.
        # 'GA "Zone Gain">1 =-42.0' (space after GA, space around '='),
        # so the patterns tolerate optional whitespace at each separator.
        self._gain_re: re.Pattern[str] = re.compile(
            r'GA\s*"(.+?) Gain"\s*>1\s*=\s*(.*)'
        )
        self._mute_re: re.Pattern[str] = re.compile(
            r'GA\s*"(.+?) Gain"\s*>2\s*=\s*(.*)'
        )
        self._source_re: re.Pattern[str] = re.compile(
            r'GA\s*"(.+?) Selector"\s*>1\s*=\s*(.*)'
        )
        # AutoVolume: 'GA "Zone AV">1=2' (On) / '=1' (Off).
        self._av_re: re.Pattern[str] = re.compile(
            r'GA\s*"(.+?) AV"\s*>1\s*=\s*(.*)'
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

    def subscribe_health(self, callback: Callable[[str], None]) -> None:
        """Register a callback for health-status notifications.

        The callback receives one of the module-level ``HEALTH_*`` strings.
        Called immediately with the current status on subscribe.
        """
        self._health_callbacks.append(callback)
        callback(self._health_status)

    def unsubscribe_health(self, callback: Callable[[str], None]) -> None:
        """Unregister a previously registered health-status callback."""
        if callback in self._health_callbacks:
            self._health_callbacks.remove(callback)

    @property
    def health_status(self) -> str:
        """Return the current health-check status string."""
        return self._health_status

    @property
    def health_zone(self) -> str | None:
        """Return the zone currently used for the health-check probe."""
        return self._health_zone

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

        # Health status follows the connection edge: the probe only runs on a
        # healthy socket, so when the socket is down the sensor reports that
        # rather than a stale probe result.
        if self._health_check_enabled:
            if not is_available:
                # Don't downgrade a terminal "can't reconnect" to the milder
                # transient state.
                if self._health_status != HEALTH_CANT_RECONNECT:
                    self._notify_health(HEALTH_SOCKET_NOT_CONNECTED)
            elif self._health_zone is not None and self._health_status in (
                HEALTH_SOCKET_NOT_CONNECTED,
                HEALTH_CANT_RECONNECT,
                HEALTH_DISABLED,
            ):
                # Reconnected: hand back to the probe (it will confirm healthy).
                self._notify_health(HEALTH_STARTING)

    def _notify_health(self, status: str) -> None:
        """Notify all registered listeners of a health-status change."""
        if self._health_status == status:
            return  # No change

        _LOGGER.info("Health check status: %s", status)
        self._health_status = status
        for callback in self._health_callbacks:
            callback(status)

    def _mark_session_alive(self) -> None:
        """Record proof that the control session is live.

        Called whenever bytes are received from the device. Any byte means the
        CSP attached this TCP socket to its control session, so we reset the
        reconnect backoff, unblock connect()'s liveness check, and mark the
        device available.
        """
        self._reconnect_attempts = 0
        self._alive_event.set()
        self._notify_availability(True)

    def _enable_keepalive(self) -> None:
        """Enable TCP keepalive so the OS detects a silently dropped peer.

        The CSP can drop a session without sending FIN/RST; without keepalive
        a blocked read() would never notice. Best-effort: any unsupported
        option is ignored so it can never break connect().
        """
        if self._writer is None:
            return
        sock = self._writer.get_extra_info("socket")
        if sock is None:
            return
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            # Per-option tuning where the platform exposes it (Linux names;
            # macOS uses TCP_KEEPALIVE for the idle time).
            idle_opt = getattr(socket, "TCP_KEEPIDLE", None) or getattr(
                socket, "TCP_KEEPALIVE", None
            )
            if idle_opt is not None:
                sock.setsockopt(socket.IPPROTO_TCP, idle_opt, 20)
            if hasattr(socket, "TCP_KEEPINTVL"):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 10)
            if hasattr(socket, "TCP_KEEPCNT"):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)
        except OSError as err:  # noqa: BLE001
            _LOGGER.debug("Could not set TCP keepalive options: %s", err)

    async def connect(self) -> None:
        """Connect to the device and start listener/query tasks.

        Serialized via ``_connect_lock`` so two concurrent callers can never
        open two TCP sockets to the single-session device at once.

        Raises:
            BoseCSPConnectionError: If the connection cannot be established.
        """
        async with self._connect_lock:
            if self.is_connected:
                _LOGGER.warning("Already connected.")
                return
            await self._do_connect()

    async def _do_connect(self) -> None:
        """Open the socket and start the listener/query tasks.

        The caller must hold ``_connect_lock``.
        """
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
            self._enable_keepalive()
            self._running = True
            self._alive_event.clear()

            self._listener_task = asyncio.create_task(self._listen())
            self._query_task = asyncio.create_task(self._periodic_query())

            # The health-check loop self-guards on connection state, so it is
            # started once and kept alive across reconnects (its 30-minute timer
            # is not reset by transient drops). _cleanup_connection deliberately
            # leaves it running; disconnect() tears it down.
            if self._health_check_enabled and (
                self._health_task is None or self._health_task.done()
            ):
                self._health_task = asyncio.create_task(self._health_check_loop())

            _LOGGER.info(
                "TCP connection to %s:%s established; verifying control "
                "session.",
                self.host,
                self.port,
            )
            # Bootstrap the state. Availability is set by _mark_session_alive()
            # when the first byte arrives, NOT on TCP-open, because the CSP may
            # accept the socket while its web configuration dashboard holds an
            # exclusive session and then never answer.
            await self.query_all_zones_state()

            # Require a real response so a held/dead session surfaces as a
            # connect failure (handed to reconnect/backoff, or surfaced to the
            # config flow) instead of a device stuck at default 0 dB / no
            # source.
            await asyncio.wait_for(
                self._alive_event.wait(), timeout=self._connect_verify_timeout
            )
            _LOGGER.info(
                "Control session with %s:%s is live.", self.host, self.port
            )

        except (OSError, asyncio.TimeoutError) as err:
            # Distinguish the common, actionable causes so the log explains why
            # the zones went unavailable rather than just "connect failed".
            if isinstance(err, asyncio.TimeoutError):
                # TCP opened (or open timed out) but no SoIP response arrived.
                _LOGGER.warning(
                    "Connected to %s:%s but the device sent no response; "
                    "another exclusive session (the CSP web configuration "
                    "dashboard) may be active. Retrying.",
                    self.host,
                    self.port,
                )
            elif getattr(err, "errno", None) == errno.ECONNREFUSED:
                _LOGGER.warning(
                    "Connection refused on %s:%s - the CSP web configuration "
                    "dashboard is likely open and holding an exclusive session. "
                    "Close it and the integration will reconnect automatically.",
                    self.host,
                    self.port,
                )
            else:
                _LOGGER.error(
                    "Failed to establish a control session with %s:%s: %s",
                    self.host,
                    self.port,
                    err,
                )
            self._notify_availability(False)
            # Tear down the half-started connection so the orphaned listener
            # can't independently schedule a reconnect. The caller decides what
            # happens next: _handle_reconnect reschedules with backoff, while
            # the config flow / coordinator surfaces the failure to HA, which
            # owns the retry. Either way there is exactly one retry path.
            await self._cleanup_connection()
            raise BoseCSPConnectionError(
                "Failed to establish a control session with %s:%s"
                % (self.host, self.port)
            ) from err

    async def disconnect(self) -> None:
        """Disconnect from the device and clean up all tasks."""
        _LOGGER.info("Disconnecting...")
        self._running = False
        self._notify_availability(False)

        # Cancel the reconnect task first and WAIT for it to unwind, so an
        # in-flight connect() can't leak a half-open socket (one that hasn't
        # been stored in self._writer yet) and keep the device's single
        # session reserved. Safe: disconnect() is only called from HA unload,
        # never from inside the reconnect task, so this cannot deadlock.
        if self._reconnect_task:
            self._reconnect_task.cancel()
            try:
                await self._reconnect_task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
            self._reconnect_task = None

        # Stop the health-check loop (kept alive across reconnects, so it is
        # torn down here at full shutdown rather than in _cleanup_connection).
        if self._health_task:
            self._health_task.cancel()
            try:
                await self._health_task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
            self._health_task = None

        # Cancel any pending optimistic ignore-flag timers.
        for task in list(self._background_tasks):
            task.cancel()

        await self._cleanup_connection()
        _LOGGER.info("Disconnected.")

    async def _cleanup_connection(self) -> None:
        """Cancel listener/query tasks and close the socket.

        Safe to call multiple times. Used by both disconnect() and
        connect() (to clean up stale state before reconnecting).
        """
        tasks = [
            task
            for task in (self._listener_task, self._query_task)
            if task and not task.done()
        ]
        for task in tasks:
            task.cancel()
        # Await cancellation so old reads/drains finish before the writer is
        # closed and a new session opens (overlapping I/O on a single-session
        # device wedges it). Safe: cleanup is only ever called from connect()/
        # disconnect(), never from inside these tasks.
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
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
        """Retry connecting (with backoff) until reconnected or stopped.

        This is a self-contained loop rather than a one-shot attempt that
        re-arms via ``_start_reconnect()``. Re-arming that way is broken: while
        this coroutine runs it *is* ``self._reconnect_task``, so the
        ``_reconnect_task.done()`` guard in ``_start_reconnect()`` would refuse
        to schedule the next attempt and the retry chain would die after a
        single failure (e.g. the CSP web dashboard holding the session). Looping
        here keeps retrying so the integration recovers on its own once the
        device frees the SoIP port.
        """
        while self._running and not self.is_connected:
            # Exponential backoff capped at ``_reconnect_backoff_max``. Without
            # the cap, a device that keeps refusing connections (e.g. its web
            # dashboard is open) would back off unboundedly.
            self._reconnect_attempts += 1
            delay = min(
                self._reconnect_delay * (2 ** (self._reconnect_attempts - 1)),
                self._reconnect_backoff_max,
            )
            _LOGGER.info("Waiting %ss before reconnecting...", delay)
            await asyncio.sleep(delay)

            if not self._running:
                return

            try:
                await self.connect()
                return  # Success; _mark_session_alive() reset the backoff.
            except asyncio.CancelledError:
                return  # Shutdown in progress, don't retry.
            except Exception:  # noqa: BLE001
                # Sustained failures escalate the health status from the milder
                # "Socket Not Connected" to the terminal "cant_reconnect".
                # _mark_session_alive() resets this once a byte arrives again.
                if (
                    self._health_check_enabled
                    and self._reconnect_attempts >= HEALTH_MAX_ATTEMPTS
                ):
                    self._notify_health(HEALTH_CANT_RECONNECT)
                # Swallow and loop so the retry never dies; the next pass waits
                # a longer backoff and tries again.
                continue

    # ------------------------------------------------------------------ #
    #  TCP listener
    # ------------------------------------------------------------------ #

    async def _listen(self) -> None:
        """Listen for incoming data and parse responses."""
        buffer = ""
        # The poll loop queries the device at least every ``volume_interval``
        # seconds, so a silence well beyond that means the session died without
        # sending FIN/RST (a known CSP behaviour). Reconnect instead of
        # blocking forever on read().
        read_timeout = max(self._volume_interval, self._other_interval) * 2 + 10
        while self._running and self._reader:
            try:
                data = await asyncio.wait_for(
                    self._reader.read(1024), timeout=read_timeout
                )
                if not data:
                    _LOGGER.warning("Connection closed by remote end.")
                    self._start_reconnect()
                    return

                # Any bytes prove the control session is live.
                self._mark_session_alive()

                buffer += data.decode("utf-8")
                while "\r" in buffer:
                    line, buffer = buffer.split("\r", 1)
                    self._parse_response(line)

            except asyncio.CancelledError:
                return
            except asyncio.TimeoutError:
                # Must precede the OSError handler: on Python 3.11+
                # asyncio.TimeoutError is a subclass of OSError.
                _LOGGER.warning(
                    "No data from %s for %ss; control session presumed dead, "
                    "reconnecting.",
                    self.host,
                    read_timeout,
                )
                self._start_reconnect()
                return
            except (OSError, ConnectionResetError) as err:
                _LOGGER.error("Connection error in listener: %s", err)
                self._start_reconnect()
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
                        if not self._ignore_av_update.get(zone):
                            await self.query_auto_volume(zone)
                        await asyncio.sleep(0.1)

                tick_count += 1
                await asyncio.sleep(self._volume_interval)

            except asyncio.CancelledError:
                return
            except Exception as err:  # noqa: BLE001
                _LOGGER.error("Error in periodic query: %s", err)
                # Back off before retrying so an unexpected error cannot
                # turn the poll loop into a tight CPU/command busy-loop.
                await asyncio.sleep(self._volume_interval)

    # ------------------------------------------------------------------ #
    #  Active "Health Checking" probe
    # ------------------------------------------------------------------ #
    #
    # Verifies the control session can actually *mutate* state (not merely
    # answer reads). One sticky, in-memory zone (never persisted) is nudged by
    # the 0.5 dB minimum step, then read back and restored, entirely off the
    # public state/event path. Failures hand off to the existing reconnect
    # machinery; status is published via the health callback.

    def _select_health_zone(self) -> str | None:
        """Pick (and remember) a zone usable for the probe.

        Returns the first zone whose AutoVolume is Off and which has at least
        one micro-step of headroom, or None if none qualifies. The choice lives
        only on this instance and is re-derived on every (re)connect.
        """
        for zone in self._zones:
            if self._state[zone].auto_volume:
                continue
            limits = self._zone_limits.get(zone)
            if limits and (limits[1] - limits[0]) < MICRO_STEP_DB:
                continue
            self._health_zone = zone
            return zone
        self._health_zone = None
        return None

    def _ensure_health_zone(self) -> bool:
        """Confirm the chosen zone is still usable, re-selecting if not.

        Returns False (and logs) when no non-AutoVolume zone remains.
        """
        zone = self._health_zone
        if zone is not None and not self._state[zone].auto_volume:
            return True
        if zone is not None:
            _LOGGER.warning(
                "Health-check zone '%s' is now AutoVolume-On; re-selecting.",
                zone,
            )
        if self._select_health_zone() is None:
            _LOGGER.error(
                "No non-AutoVolume zone available for health checking; "
                "stopping the probe."
            )
            return False
        return True

    def _nudge_target(self, zone: str, current: float) -> float:
        """Compute the micro-adjusted target, respecting floor/ceiling."""
        up = round(current + MICRO_STEP_DB, 1)
        down = round(current - MICRO_STEP_DB, 1)
        limits = self._zone_limits.get(zone)
        if limits:
            floor, ceiling = limits
            if up > ceiling:
                return down
            if down < floor:
                return up
        return up

    async def _run_single_check(self) -> str:
        """Run one nudge/readback/restore probe.

        Returns "pass", "fail", or "inconclusive" (a user/external change during
        the window — value left untouched, never counted as a failure).
        """
        zone = self._health_zone
        original = self._state[zone].volume
        target = self._nudge_target(zone, original)

        self._probe_zone = zone
        self._health_user_change = False
        self._probe_value = None
        result = "inconclusive"
        try:
            # Nudge behind the scenes (no optimistic update, no callback).
            await self._send_command('SA "%s Gain">1=%.1f' % (zone, target))
            await asyncio.sleep(HEALTH_WAIT)

            # Only judge a result on a still-healthy socket; otherwise the
            # connection layer owns the status and this is inconclusive.
            if not self.is_connected:
                return "inconclusive"

            self._probe_event.clear()
            self._probe_value = None
            await self.query_volume(zone)
            try:
                await asyncio.wait_for(self._probe_event.wait(), timeout=5)
            except asyncio.TimeoutError:
                pass

            value = self._probe_value
            if self._health_user_change or (
                value is not None and value != target and value != original
            ):
                result = "inconclusive"
            elif value == target:
                result = "pass"
            else:
                result = "fail"  # value == original, or no readback
            return result
        finally:
            self._probe_zone = None
            # Always restore for pass/fail so the level never drifts; never
            # clobber a deliberate user/external change (inconclusive).
            if result in ("pass", "fail") and self.is_connected:
                await self._send_command(
                    'SA "%s Gain">1=%.1f' % (zone, original)
                )

    async def _run_health_cycle(self) -> None:
        """Run one cycle: up to HEALTH_MAX_ATTEMPTS, else force a reconnect."""
        self._notify_health(HEALTH_CHECKING)
        for attempt in range(1, HEALTH_MAX_ATTEMPTS + 1):
            if not self.is_connected:
                return  # Socket dropped mid-cycle; connection layer owns status.
            result = await self._run_single_check()
            if result == "pass":
                self._notify_health(HEALTH_HEALTHY)
                return
            if result == "inconclusive":
                # A user/external change; can't verify this cycle, try next time.
                _LOGGER.debug(
                    "Health check inconclusive (volume changed during window); "
                    "rescheduling."
                )
                return
            _LOGGER.warning(
                "Health check attempt %d/%d failed for zone '%s'.",
                attempt,
                HEALTH_MAX_ATTEMPTS,
                self._health_zone,
            )
            self._notify_health(HEALTH_FAILING)
            if attempt < HEALTH_MAX_ATTEMPTS:
                await asyncio.sleep(HEALTH_RETRY_DELAY)

        _LOGGER.error(
            "Health check failed %d times; control session unverified, "
            "forcing reconnect.",
            HEALTH_MAX_ATTEMPTS,
        )
        self._start_reconnect()

    async def _health_check_loop(self) -> None:
        """Periodic probe loop. Started on connect; survives reconnects."""
        if not self._health_check_enabled:
            return
        # Let the bootstrap poll populate AutoVolume state before choosing.
        await asyncio.sleep(5)
        if self._select_health_zone() is None:
            self._notify_health(HEALTH_NO_ZONE)
            return
        if self._health_status not in (
            HEALTH_SOCKET_NOT_CONNECTED,
            HEALTH_CANT_RECONNECT,
        ):
            self._notify_health(HEALTH_STARTING)

        while self._running:
            try:
                await asyncio.sleep(HEALTH_INTERVAL)
                if not self._running:
                    return
                # Only probe an assumed-healthy connection.
                if not self.is_connected:
                    continue
                if not self._ensure_health_zone():
                    self._notify_health(HEALTH_NO_ZONE)
                    return  # No usable zone; stop running.
                await self._run_health_cycle()
            except asyncio.CancelledError:
                return
            except Exception as err:  # noqa: BLE001
                _LOGGER.error("Error in health check loop: %s", err)
                await asyncio.sleep(HEALTH_RETRY_DELAY)

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
                # Health-check probe interception: while a probe is in flight
                # for this zone, route the gain readback to the probe channel
                # and do NOT touch public state or fire callbacks, so the probe
                # is invisible to Home Assistant.
                if area == self._probe_zone:
                    try:
                        self._probe_value = float(value.strip())
                    except ValueError:
                        self._probe_value = None
                    self._probe_event.set()
                    return
                # Check ignore flag
                if self._ignore_volume_update.get(area):
                    _LOGGER.debug(
                        "Ignoring volume update for %s due to debounce", area
                    )
                    return
                if area in self._state:
                    new_vol = float(value.strip())
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
                    new_mute = value.strip() == "O"
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
                    new_source = int(value.strip())
                    if self._state[area].current_source != new_source:
                        self._state[area].current_source = new_source
                        updated_zone = area
            elif m := self._av_re.match(response):
                area, value = m.groups()
                if self._ignore_av_update.get(area):
                    _LOGGER.debug(
                        "Ignoring AutoVolume update for %s due to debounce", area
                    )
                    return
                if area in self._state:
                    # '2' = AutoVolume On, '1' = Off.
                    new_av = value.strip() == "2"
                    if self._state[area].auto_volume != new_av:
                        self._state[area].auto_volume = new_av
                        updated_zone = area

        except Exception as err:  # noqa: BLE001
            _LOGGER.error("Error parsing response '%s': %s", response, err)

        if updated_zone:
            # Availability is already handled in _listen() (any byte ->
            # _mark_session_alive); here we only fan out the state change.
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
        # Guard on the writer only (not is_connected): connect() must be able
        # to send the bootstrap queries that prove liveness before
        # availability is set.
        if not self._writer:
            _LOGGER.warning("No active connection. Command not sent: %s", command)
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

    def _spawn_ignore_flag(
        self, flag_dict: dict[str, bool], key: str, delay: float
    ) -> None:
        """Spawn a tracked ignore-flag timer task.

        The task is kept in ``_background_tasks`` until it finishes so it is
        not garbage-collected mid-flight (a known asyncio foot-gun).
        """
        task = asyncio.create_task(
            self._set_ignore_flag(flag_dict, key, delay)
        )
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    # ------------------------------------------------------------------ #
    #  Public control methods
    # ------------------------------------------------------------------ #

    async def set_volume(self, zone_name: str, volume_db: float) -> None:
        """Set the gain level for a zone and optimistically update state.

        Args:
            zone_name: The zone to control.
            volume_db: The desired volume in dB.
        """
        # The device rejects gain sets while AutoVolume is On, so don't send a
        # doomed command or apply a phantom optimistic update. Re-fire the
        # callback so any UI snaps back to the AutoVolume-driven level.
        if self._state[zone_name].auto_volume:
            _LOGGER.warning(
                "Ignoring volume change for '%s': AutoVolume is On (the device "
                "controls the level). Turn AutoVolume off to set volume.",
                zone_name,
            )
            self._fire_update_callback(zone_name)
            return

        # If a health-check probe is mid-window on this zone, a user-originated
        # change must not be read back as a failed probe; flag it so the probe
        # classifies the cycle as inconclusive and leaves the user's value.
        if zone_name == self._probe_zone:
            self._health_user_change = True

        # Only send if state is different
        if self._state[zone_name].volume == volume_db:
            return

        # Set ignore flag
        self._spawn_ignore_flag(self._ignore_volume_update, zone_name, 2.0)

        # Optimistic update (before await to be synchronous)
        self._state[zone_name].volume = volume_db
        self._fire_update_callback(zone_name)

        cmd = 'SA "%s Gain">1=%.1f' % (zone_name, volume_db)
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
        self._spawn_ignore_flag(self._ignore_mute_update, zone_name, 2.0)

        # Optimistic update (before await to be synchronous)
        self._state[zone_name].is_muted = mute_on
        self._fire_update_callback(zone_name)

        state_char = "O" if mute_on else "F"
        cmd = 'SA "%s Gain">2=%s' % (zone_name, state_char)
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
        self._spawn_ignore_flag(self._ignore_source_update, zone_name, 2.0)

        # Optimistic update (before await to be synchronous)
        self._state[zone_name].current_source = source_index
        self._fire_update_callback(zone_name)

        cmd = 'SA "%s Selector">1=%s' % (zone_name, source_index)
        await self._send_command(cmd)

    async def set_auto_volume(self, zone_name: str, enabled: bool) -> None:
        """Enable or disable AutoVolume for a zone.

        Setting AutoVolume is only accepted by the device when the zone has been
        AutoVolume-calibrated; otherwise the command is NAK'd and the next poll
        re-syncs the real state.

        Args:
            zone_name: The zone to control.
            enabled: True to turn AutoVolume On, False to turn it Off.
        """
        # Only send if state is different
        if self._state[zone_name].auto_volume == enabled:
            return

        # Set ignore flag
        self._spawn_ignore_flag(self._ignore_av_update, zone_name, 2.0)

        # Optimistic update (before await to be synchronous)
        self._state[zone_name].auto_volume = enabled
        self._fire_update_callback(zone_name)

        # '2' = AutoVolume On, '1' = Off.
        cmd = 'SA "%s AV">1=%s' % (zone_name, "2" if enabled else "1")
        await self._send_command(cmd)

    # ------------------------------------------------------------------ #
    #  Public query methods
    # ------------------------------------------------------------------ #

    async def query_volume(self, zone_name: str) -> None:
        """Query the current volume for a zone."""
        cmd = 'GA "%s Gain">1' % zone_name
        await self._send_command(cmd)

    async def query_mute(self, zone_name: str) -> None:
        """Query the current mute state for a zone."""
        cmd = 'GA "%s Gain">2' % zone_name
        await self._send_command(cmd)

    async def query_source(self, zone_name: str) -> None:
        """Query the current source selection for a zone."""
        cmd = 'GA "%s Selector">1' % zone_name
        await self._send_command(cmd)

    async def query_auto_volume(self, zone_name: str) -> None:
        """Query the current AutoVolume state for a zone."""
        cmd = 'GA "%s AV">1' % zone_name
        await self._send_command(cmd)

    async def query_all_zones_state(self) -> None:
        """Query volume, mute, source, and AutoVolume for all zones."""
        _LOGGER.info("Querying all device states...")
        for zone in self._zones:
            await self.query_volume(zone)
            await asyncio.sleep(0.1)
            await self.query_mute(zone)
            await asyncio.sleep(0.1)
            await self.query_source(zone)
            await asyncio.sleep(0.1)
            await self.query_auto_volume(zone)
            await asyncio.sleep(0.1)
